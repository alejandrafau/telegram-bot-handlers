
import logging
import os
import asyncio
import re
import pandas as pd
from telegram import Bot
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
import handler_subscriber.models as mod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()
bot_token = os.getenv("BOT_TOKEN")
logger = logging.getLogger(__name__)

class Broadcaster():
    def __init__(self):
        self.db_engine = None
        self.db_session = None
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.persistance_directory = os.path.join(base_dir, "..", "assets")
        self.bot = Bot(token=bot_token)
        self.events = None

    def set_events(self,events_dict):
        self.events = events_dict

# --- Tema ---
    def get_users_by_tema(self, tema: str) -> list[int]:
        db = self.db_session
        """Devuelve los user_id suscriptos a un tema específico"""
        rows = db.query(mod.SuscripcionTema.user_id).filter_by(tema=tema).all()
        return [r[0] for r in rows]

    # --- Dataset ---
    def get_users_by_dataset(self, dataset: str) -> list[int]:
        """Devuelve los user_id suscriptos a un dataset específico"""
        db = self.db_session
        rows = db.query(mod.SuscripcionDataset.user_id).filter_by(dataset=dataset).all()
        return [r[0] for r in rows]

    # --- Nodo ---
    def get_users_by_nodo(self, nodo: str) -> list[int]:
        """Devuelve los user_id suscriptos a un nodo específico"""
        db = self.db_session
        rows = db.query(mod.SuscripcionNodo.user_id).filter_by(nodo=nodo).all()
        return [r[0] for r in rows]

    def escape_markdown(self,text: str) -> str:
        """
        Escapa caracteres especiales de MarkdownV2
        """
        if text is None or text == "":
            return ""
        return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)


    async def send_new_dataset_message(self):
        """Envia mensaje de nuevo dataset a suscriptores de tema y nodo al que pertenece el dataset"""
        if not self.events:
            return
        else:
            dataset_event = self.events.get("dataset_event")
            if isinstance(dataset_event,pd.DataFrame) and len(dataset_event)>0:
                datasets = dataset_event["dataset_id"].unique().tolist()
                for dataset in datasets:
                    df = dataset_event.loc[dataset_event["dataset_id"]==dataset]
                    nodos = df['nodo_alias'].unique().tolist()
                    themes = df['temas_alias'].unique().tolist()
                    maintainer = self.escape_markdown(df["maintainer"].iloc[0])
                    title = self.escape_markdown(df["dataset_title"].iloc[0])
                    url = df["url"].iloc[0]
                    message = f"{maintainer} publicó un nuevo dataset: [{title}]({url})"
                    nodo_subs = []
                    theme_subs =[]
                    for nodo in nodos:
                       users = self.get_users_by_nodo(nodo)
                       nodo_subs.append(users)
                    for theme in themes:
                        users = self.get_users_by_tema(theme)
                        theme_subs.append(users)
                    total_subs = set()
                    for users in nodo_subs + theme_subs:
                        total_subs.update(users)
                    if total_subs:
                        await self.send_update(total_subs, message)

    async def send_new_distribution_message(self):
        """Envia mensaje de nuevo recurso a suscriptores de tema, nodo o dataset al que pertenece el recurso"""
        if not self.events:
            return
        else:
            distri_event = self.events.get("distri_event")
            if isinstance(distri_event,pd.DataFrame) and len(distri_event)>0:
                distributions = distri_event["distribution_id"].unique().tolist()
                for distri in distributions:
                    df = distri_event.loc[distri_event["distribution_id"]==distri]
                    name = self.escape_markdown(df["distribution_name"].iloc[0])
                    nodos = df['nodo_alias'].unique().tolist()
                    themes = df['temas_alias'].unique().tolist()
                    dataset_id = df['dataset_id'].iloc[0]
                    maintainer = self.escape_markdown(df["maintainer"].iloc[0])
                    dataset_title = self.escape_markdown(df["dataset_title"].iloc[0])
                    url = df["url"].iloc[0]
                    message = f"{maintainer} publicó un nuevo recurso: [{name}]({url}) dentro del dataset {dataset_title}"
                    nodo_subs = []
                    theme_subs =[]
                    for nodo in nodos:
                       users = self.get_users_by_nodo(nodo)
                       nodo_subs.append(users)
                    for theme in themes:
                        users = self.get_users_by_tema(theme)
                        theme_subs.append(users)
                    data_sub = self.get_users_by_dataset(dataset_id)
                    total_subs = set()
                    for users in nodo_subs:
                        total_subs.update(users)
                    for users in theme_subs:
                        total_subs.update(users)
                    total_subs.update(data_sub)
                    if total_subs:
                        await self.send_update(total_subs, message)

    async def send_new_datapoint_message(self):
        if not self.events:
            return
        else:
            dp_event = self.events.get("datapoint_event")
            if isinstance(dp_event,pd.DataFrame) and len(dp_event)>0:
                distributions = dp_event["distribution_id"].unique().tolist()
                for distri in distributions:
                    df = dp_event.loc[dp_event["distribution_id"]==distri]
                    name = self.escape_markdown(df["distribution_name"].iloc[0])
                    dataset_id = df["dataset_id"].iloc[0]
                    maintainer = self.escape_markdown(df["maintainer"].iloc[0])
                    dataset_title = self.escape_markdown(df["dataset_title"].iloc[0])
                    url = df["url"].iloc[0]
                    message = f"{maintainer} agregó nuevos datos al recurso [{name}]({url}) dentro del dataset {dataset_title}"
                    total_subs = self.get_users_by_dataset(dataset_id)
                    await self.send_update(total_subs,message)

    async def send_update(self, total_subs, message):
        bot = self.bot
        for user_id in total_subs:
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=message,
                    parse_mode="MarkdownV2"
                )
            except Exception as e:
                logger.error(f"Failed to send message to {user_id}: {e}")
                raise e


    def send_email_report(self,sender_email, sender_password, recipient_email, subject, body, attachment_paths=None):
        msg = EmailMessage()
        msg['From'] = sender_email
        if isinstance(recipient_email, str):
            recipient_email = [recipient_email]
        msg['To'] = ', '.join(recipient_email)
        msg['Subject'] = subject
        msg.set_content(body)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(sender_email, sender_password)
            smtp.send_message(msg)
            print("Email enviado!")
