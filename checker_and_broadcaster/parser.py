import requests
import yaml
import json
import logging
import pandas as pd
import os
from checker_and_broadcaster import utils
from checker_and_broadcaster import distribution_processor as dp
from dotenv import load_dotenv
import handler_subscriber.models as mod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()
bot_token = os.getenv("BOT_TOKEN")
database_url = os.getenv("DATABASE_URL")
logger = logging.getLogger(__name__)

class Parser:
   def __init__(self,db_engine,session_class):
      self.db_engine = db_engine
      self.db_session = session_class()
      base_dir = os.path.dirname(os.path.abspath(__file__))
      self.persistance_directory = os.path.join(base_dir, "..", "assets")
      os.makedirs(self.persistance_directory, exist_ok=True)
      self.missing_dataset_ids = self._get_missing_ids()
      self.missing_dataset_titles = self._get_missing_titles()
      self.connection_errors = None
      self.parseable_datasets = self._fetch_datasets_to_parse()
      self.previous_state = self._get_previous_state()
      self.current_state = self._get_current_state()
      self.colnames_ds_events = ["dataset_id", "dataset_title", "temas_alias", "nodo_alias", "maintainer", "url",
                                 "event_type"]
      self.colnames_dist_events = ["distribution_id", "distribution_name"] + self.colnames_ds_events
      self.dataset_events = self._get_dataset_events()
      self.distribution_events = self._get_distribution_events()
      self.datapoint_events = self._get_datapoint_events()

   def _fetch_datasets_to_parse(self):
      try:
          with self.db_session:
             results = (
                self.db_session.query(mod.SuscripcionDataset.dataset)
                .distinct()
                .all()
             )
             return [r[0] for r in results] if results else []
      except:
          logger.error("No se pudo conectar con la tabla SuscripcionDataset para buscar datasets a parsear")
          return []

   def _get_missing_ids(self):
      try:
         missing_path = os.path.join(self.persistance_directory,"missings.json")
         missing = utils.read_json(missing_path)
         missing_ids = list(missing.keys())
         return missing_ids
      except:
         return []

   def _get_missing_titles(self):
      try:
         missing_path = os.path.join(self.persistance_directory,"missings.json")
         missing = utils.read_json(missing_path)
         missing_titles = list(missing.values())
         return missing_titles
      except:
         return []

   def _update_missings(self, found_ids, found_titles):
       missing_path = os.path.join(self.persistance_directory, "missings.json")
       if os.path.exists(missing_path):
           missing = utils.read_json(missing_path)
       else:
           missing = {}
       final_missing = {
           k: v for k, v in missing.items()
           if k not in found_ids and v not in found_titles
       }
       utils.write_json(missing_path, final_missing)

   def _get_previous_state(self):
         state_path = os.path.join(self.persistance_directory, "last_ckan_state.json")
         state = utils.read_json(state_path)
         return state

   def _get_current_state(self):
       try:
           full_datasets = self._get_raw_state()
           total_dist = sum(len(v["resources"]) for v in full_datasets.values())
           ckan_state = {"total_datasets": len(full_datasets),
                         "total_distributions": total_dist,
                         "data": {}}
           full_distributions = {}
           data = ckan_state["data"]

           for dataset_id, dataset_attr in full_datasets.items():
               data[dataset_id] = {"org": {},
                                   "temas": {},
                                   "title": dataset_attr['title'],
                                   "name": dataset_attr['name'],
                                   "distributions": {}}

               data[dataset_id]['org']['maintainer'] = dataset_attr['maintainer']
               data[dataset_id]['org']['nodo_title'] = dataset_attr['organization']['title']
               data[dataset_id]['org']['nodo_alias'] = dataset_attr['organization']['name']
               data[dataset_id]['temas']['temas_alias'] = [
                   g['name'] for g in dataset_attr['groups']
               ]
               data[dataset_id]['temas']['temas_nombres'] = [
                   g['display_name'] for g in dataset_attr['groups']
               ]

               dataset_distributions = {}
               for distribution in dataset_attr['resources']:
                   id = distribution['id']
                   dataset_distributions[id] = {
                       "url": distribution['url'],
                       "name": distribution['name'],
                       "size": None
                   }
                   if dataset_id in self.parseable_datasets:
                       full_distributions[id] = {
                           "url": distribution['url'],
                           "size": None
                       }
               data[dataset_id]['distributions'] = dataset_distributions

           processor = dp.DistributionProcessor()
           results = processor.process_distributions_concurrent(full_distributions)
           results_df = pd.DataFrame(results, columns=["distribution_id", "url", "size", "error"])
           errors = results_df.loc[results_df['error'].notna() & (results_df['error'] != "")]
           print(len(errors))
           self.connection_errors = errors


           for dataset_attr in ckan_state['data'].values():
               for distribution_id, distribution_attr in dataset_attr['distributions'].items():
                   # Filtrar resultados para esta distribución
                   mask = results_df["distribution_id"] == distribution_id
                   matching_rows = results_df[mask]

                   if len(matching_rows) == 0:
                       distribution_attr['size'] = None
                       continue

                   size_value = matching_rows.iloc[0]['size']


                   if hasattr(size_value, 'item'):
                       try:
                           distribution_attr['size'] = size_value.item()
                       except:
                           distribution_attr['size'] = float(size_value) if size_value is not None else None
                   elif pd.isna(size_value):
                       distribution_attr['size'] = None
                   else:
                       distribution_attr['size'] = float(size_value) if size_value is not None else None

           return ckan_state
       except Exception as e:
           raise e

   def _get_dataset_events(self):
      final_updates = None
      if self.previous_state and self.current_state:
            prev_datasets = list(self.previous_state.get('data', {}).keys())
            curr_datasets = list(self.current_state.get('data', {}).keys())
            new_datasets = list(set(curr_datasets) - set(prev_datasets))
            absent_datasets = list(set(prev_datasets)-set(curr_datasets))

            if len(new_datasets)>0:
               rows = []
               base_url = "https://datos.gob.ar/dataset/"
               curr_data = self.current_state.get("data", {})
               for element in new_datasets:
                  dataset = curr_data.get(element, {})
                  rows.append({
                     "dataset_id": element,
                     "dataset_title": dataset.get("title"),
                     "temas_alias": dataset.get("temas", {}).get("temas_alias"),
                     "nodo_alias": dataset.get("org", {}).get("nodo_alias"),
                     "maintainer": dataset.get("org", {}).get("maintainer"),
                     "url": base_url + dataset["name"] if isinstance(dataset.get("name"), str) else None,
                     "event_type": "nuevo_dataset"
                  })

               dataset_updates = pd.DataFrame(rows, columns=self.colnames_ds_events)

               filt_updates_a = dataset_updates.loc[
                  ~dataset_updates["dataset_id"].isin(self.missing_dataset_ids)
               ]
               filt_updates_b = filt_updates_a.loc[
                  ~filt_updates_a["dataset_title"].isin(self.missing_dataset_titles)
               ]
               final_updates = filt_updates_b.explode("temas_alias", ignore_index=True)
               found_ids = set(dataset_updates["dataset_id"].tolist())
               found_titles = set(dataset_updates["dataset_title"].tolist())

               self._update_missings(found_ids, found_titles)


            if len(absent_datasets)>0:
               diff_datasets = list(set(prev_datasets) - set(curr_datasets))
               prev_data = self.previous_state.get("data", {})
               missing_path = os.path.join(self.persistance_directory, "missings.json")
               os.makedirs(os.path.dirname(missing_path), exist_ok=True)
               if not os.path.exists(missing_path):
                   c_missings = {}
               else:
                   c_missings = utils.read_json(missing_path)

               for element in diff_datasets:
                   dataset = prev_data.get(element, {})
                   c_missings[element] = dataset.get("title")
               utils.write_json(missing_path, c_missings)
      return final_updates

   def _get_distribution_events(self):
      final_dist_updates = None
      if self.previous_state and self.current_state:

            prev_distributions = [
               element
               for _, v in self.previous_state.get('data').items()
               for element in v['distributions']
            ]
            curr_distributions = [
               element
               for _, v in self.current_state.get('data').items()
               for element in v['distributions']
            ]

            new_distributions = list(set(curr_distributions) - set(prev_distributions))

            if len(new_distributions)>0:
               rows = []
               curr_data = self.current_state.get("data", {})

               for element in new_distributions:
                  for dataset_id, dataset_info in curr_data.items():
                     for dist_id, dist_info in dataset_info.get('distributions', {}).items():
                        if dist_id == element:
                           rows.append({
                              "distribution_id": dist_id,
                              "distribution_name": dist_info['name'],
                              "dataset_id": dataset_id,
                              "dataset_title": dataset_info['title'],
                              "temas_alias": dataset_info['temas']['temas_alias'],
                              "nodo_alias": dataset_info['org']['nodo_alias'],
                              "maintainer": dataset_info['org']['maintainer'],
                              "url": dist_info['url'],
                              "event_type": "nueva_distribucion"
                           })
               new_datasets = (
                  self.dataset_events['dataset_id'].unique().tolist()
                  if isinstance(self.dataset_events, pd.DataFrame) and len(self.dataset_events) > 0
                  else []
               )
               distribution_updates = pd.DataFrame(rows, columns=self.colnames_dist_events)
               distribution_updates = distribution_updates[
                  ~distribution_updates['dataset_id'].isin(new_datasets)
               ]
               distribution_updates = distribution_updates[
                  ~distribution_updates['dataset_id'].isin(self.missing_dataset_ids)
               ]
               distribution_updates = distribution_updates[
                  ~distribution_updates['dataset_title'].isin(self.missing_dataset_titles)
               ]

               final_dist_updates = distribution_updates.explode("temas_alias", ignore_index=True)

      return final_dist_updates

   def _get_raw_state(self):
      """Esta función se conecta con datos.gob.ar y devuelve un diccionario con información de todos los datasets
      si la conexión fue existosa y si no un diccionario vacio"""
      urls = [
         "https://datos.gob.ar/api/3/action/package_search?rows=1000",
         "https://datos.gob.ar/api/3/action/package_search?rows=1000&start=1000"
      ]

      try:
         full_datasets = {
            dataset['id']: {k: v for k, v in dataset.items()}
            for url in urls
            for dataset in requests.get(url).json()['result']['results']
         }
         return full_datasets
      except Exception as e:
         raise e

   def _get_datapoint_events(self):
      if self.previous_state and self.current_state:
         prev_distributions = [
            element
            for _, v in self.previous_state.get('data', {}).items()
            for element in v.get('distributions', [])
         ]
         curr_distributions = [
            element
            for _, v in self.current_state.get('data', {}).items()
            for element in v.get('distributions', [])
         ]
         common_distributions = set(prev_distributions).intersection(set(curr_distributions))

         def _find_distribution_info(data, dist_id):
            """Devuelve (size, dist_info, dataset_id, dataset_info) de una distribución si existe."""
            for dataset_id, dataset_info in data.items():
               distributions = dataset_info.get("distributions", {})
               if dist_id in distributions:
                  d_info = distributions[dist_id]
                  size = d_info.get("size")


                  if hasattr(size, 'item'):  # Para Series/DataArray
                     try:
                        size = size.item()
                     except ValueError:
                        # Si el Series tiene más de un elemento
                        if hasattr(size, '__len__') and len(size) == 1:
                           size = size.iloc[0] if hasattr(size, 'iloc') else size[0]
                        else:
                           size = None
                  elif hasattr(size, '__len__') and not isinstance(size, (str, bytes)):
                     # Para arrays/listas
                     if len(size) == 1:
                        size = size[0]
                     elif len(size) == 0:
                        size = None


                  if size is not None:
                     try:

                        size = float(size)
                     except (ValueError, TypeError):

                        size = None

                  return size, d_info, dataset_id, dataset_info
            return None, None, None, None

         rows = []
         curr_data = self.current_state.get("data", {})
         prev_data = self.previous_state.get("data", {})

         for dist_id in common_distributions:
            curr_size, curr_dist_info, curr_dataset_id, curr_dataset_info = _find_distribution_info(curr_data, dist_id)
            prev_size, _, _, _ = _find_distribution_info(prev_data, dist_id)


            if (prev_size is not None and curr_size is not None and
                    isinstance(curr_size, (int, float)) and isinstance(prev_size, (int, float))):


               if curr_size > prev_size:
                  rows.append({
                     "distribution_id": dist_id,
                     "distribution_name": curr_dist_info.get("name", ""),
                     "dataset_id": curr_dataset_id or "",
                     "dataset_title": curr_dataset_info.get("title", "") if curr_dataset_info else "",
                     "temas_alias": curr_dataset_info.get("temas", {}).get("temas_alias",
                                                                           "") if curr_dataset_info else "",
                     "nodo_alias": curr_dataset_info.get("org", {}).get("nodo_alias", "") if curr_dataset_info else "",
                     "maintainer": curr_dataset_info.get("org", {}).get("maintainer", "") if curr_dataset_info else "",
                     "url": curr_dist_info.get("url", "") if curr_dist_info else "",
                     "event_type": "nuevo_datapoint",
                  })


         return pd.DataFrame(rows, columns=self.colnames_dist_events) if rows else None
      else:
         logger.info("No se detectaron eventos de datapoints: falta alguno de los estados")
         return None

   def dump_current_themes(self):
      if self.current_state:
         superthemes = []
         aliases = []
         for dataset_id, dataset_atrr in self.current_state.get('data').items():
            superthemes.append(dataset_atrr['temas']['temas_nombres'])
            aliases.append(dataset_atrr['temas']['temas_alias'])
         flat_themes = [t for theme_list in superthemes for t in theme_list]
         flat_aliases = [a for alias_list in aliases for a in alias_list]
         alias_themes = dict(zip(flat_aliases, flat_themes))
         theme_path = os.path.join(self.persistance_directory, "superthemes.yaml")
         with open(theme_path, "w", encoding="utf-8") as f:
            yaml.dump(alias_themes, f, allow_unicode=True, default_flow_style=False)
      else:
         return

   def dump_current_datasets(self):
      if not self.current_state:
         return
      else:
         current_datasets = {}
         for dataset_id, dataset_atrr in self.current_state.get('data').items():
            current_datasets[dataset_id] = dataset_atrr['name']
         dataset_path = os.path.join(self.persistance_directory, "datasets.yaml")
         with open(dataset_path, "w", encoding="utf-8") as f:
            yaml.dump(current_datasets, f, sort_keys=False, allow_unicode=True)


   def dump_current_nodes(self):
      if not self.current_state:
         return
      else:
         nodes = []
         aliases = []
         for dataset_id, dataset_atrr in self.current_state.get('data').items():
            nodes.append(dataset_atrr['org']['nodo_title'])
            aliases.append(dataset_atrr['org']['nodo_alias'])
         nodo_alias = dict(zip(aliases,nodes))
         nodo_path = os.path.join(self.persistance_directory, "organizations.yaml")
         with open(nodo_path, "w", encoding="utf-8") as f:
            yaml.dump(nodo_alias, f, allow_unicode=True, default_flow_style=False)

   def dump_error_report(self):
         error_path = os.path.join(self.persistance_directory, "error_report.csv")
         if isinstance(self.connection_errors,pd.DataFrame):
            self.connection_errors.to_csv(error_path,index=False)
         else:
            return

   def save_current_state(self):
      if not self.current_state:
         return
      else:
         state_path = os.path.join(self.persistance_directory, "last_ckan_state.json")
         with open(state_path, "w", encoding="utf-8") as f:
            json.dump(self.current_state, f, sort_keys=False, ensure_ascii=False)


   def serialize_events(self):
      event_dict = {"dataset_event":self.dataset_events,
                    "distri_event":self.distribution_events,
                    "datapoint_event":self.datapoint_events,
                    "errors":self.connection_errors}
      return event_dict






