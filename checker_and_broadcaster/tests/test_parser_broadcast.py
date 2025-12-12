import pandas as pd
import checker_and_broadcaster.parser as pars
import checker_and_broadcaster.broadcast as broad
import json
from checker_and_broadcaster.utils import read_json,write_json
import os
from pathlib import Path


current_state_path = "test_current_ckan_state.json"
previous_state_path = "test_last_ckan_state.json"


class TestParser(pars.Parser):
        def __init__(self):
            base_dir = Path(__file__).resolve().parent
            self.persistance_directory = base_dir/ "test_assets"
            self.restitute_missing()
            os.makedirs(self.persistance_directory, exist_ok=True)
            self.previous_state = read_json(self.persistance_directory / previous_state_path)
            self.current_state = read_json(self.persistance_directory / current_state_path)
            self.colnames_ds_events = ["dataset_id", "dataset_title", "temas_alias", "nodo_alias", "maintainer", "url",
                                       "event_type"]
            self.colnames_dist_events = ["distribution_id", "distribution_name"] + self.colnames_ds_events
            self.missing_dataset_ids = self._get_missing_ids()
            self.missing_dataset_titles = self._get_missing_titles()
            self.dataset_events = self._get_dataset_events()
            self.distribution_events = self._get_distribution_events()
            self.datapoint_events = self._get_datapoint_events()

        def restitute_missing(self):
              missing={"energia_1c181390-5045-475e-94dc-410429be4b17":"Precios en Surtidor - Resolución 314/2016"}
              write_json(self.persistance_directory/"missings.json",missing)

        def test_serialize_events(self):
            event_dict = {"dataset_event": self.dataset_events,
                          "distri_event": self.distribution_events,
                         }
            return event_dict

class TestBroadcaster(broad.Broadcaster):
    def test_get_users_by_tema(self):
        return ["Luis","Pablo","Adriana"]

    def test_get_users_by_nodo(self):
        return ["Luis","Pablo","Carmela"]

    def test_new_dataset_message(self):
        """Debe eliminar duplicados de suscriptores para que no reciban dos veces la misma notificacion"""
        if not self.events:
            return
        else:
            dataset_event = self.events.get("dataset_event")
            if isinstance(dataset_event,pd.DataFrame) and len(dataset_event)>0:
                datasets = dataset_event["dataset_id"].unique().tolist()
                for dataset in datasets:
                    df = dataset_event.loc[dataset_event["dataset_id"]==dataset]
                    maintainer = self.escape_markdown(df["maintainer"].iloc[0])
                    title = self.escape_markdown(df["dataset_title"].iloc[0])
                    url = df["url"]
                    message = f"{maintainer} publicó un nuevo dataset: [{title}]({url})"
                    nodo_subs = []
                    theme_subs =[]
                    nodo_users = self.test_get_users_by_nodo()
                    nodo_subs.append(nodo_users)
                    tema_users = self.test_get_users_by_tema()
                    theme_subs.append(tema_users)
                    total_subs = set()
                    for users in nodo_subs + theme_subs:
                        total_subs.update(users)
                    return total_subs




def test_parser_broadcaster():
    """Para este test se comparan test_current_ckan_state y test_last_ckan_state. Debería detectar
    que si bien hay dos datasets 'nuevos', uno de ellos está en la lista de faltantes (missings.json) - datasets que ya existian
    y se dieron de baja circunstancialmente - por lo que sólo debería identificar uno como novedad """
    test_parser = TestParser()
    test_parser.restitute_missing()
    events = test_parser.test_serialize_events()
    assert isinstance(test_parser.dataset_events,pd.DataFrame)
    assert len(test_parser.dataset_events) == 1
    assert isinstance(test_parser.distribution_events, pd.DataFrame)
    assert len(test_parser.distribution_events) == 1
    assert isinstance(test_parser.datapoint_events, pd.DataFrame)
    assert len(test_parser.datapoint_events) == 1

    test_broadcaster = TestBroadcaster()
    test_broadcaster.set_events(events)
    usuarios = test_broadcaster.test_new_dataset_message()
    assert usuarios == {"Luis", "Pablo", "Adriana", "Carmela"}
