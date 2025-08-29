import logging
import os
import yaml
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
from dotenv import load_dotenv
from validators import valid_theme,valid_dataset,extract_dataset_from_url,dataset_exists
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base,SuscripcionTema,SuscripcionDataset
import logging

load_dotenv()
bot_token = os.getenv("BOT_TOKEN")
database_url = os.getenv("DATABASE_URL")
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
with open("superthemes.yaml", "r", encoding="utf-8") as f:
    superthemes = yaml.safe_load(f)
with open("datasets.yaml","r",encoding="utf-8") as f:
    av_datasets = yaml.safe_load(f)

def guardar_suscripcion_tema(user_id: int, tema: str):
    db = SessionLocal()
    try:
        suscripcion = SuscripcionTema(user_id=user_id, tema=tema)
        db.add(suscripcion)
        db.commit()
        db.refresh(suscripcion)
        return suscripcion
    except Exception as e:
        db.rollback()
        print("Error guardando suscripción:", e)
    finally:
        db.close()

def guardar_suscripcion_dataset(user_id: int, dataset: str):
    db = SessionLocal()
    try:
        suscripcion = SuscripcionDataset(user_id=user_id, dataset=dataset)
        db.add(suscripcion)
        db.commit()
        db.refresh(suscripcion)
        return suscripcion
    except Exception as e:
        db.rollback()
        print("Error guardando suscripción:", e)
    finally:
        db.close()



async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="👋 ¡Hola! Soy DATOB 🤖\n\n"
             "📢 Actualmente mando notificaciones en el <a href='https://t.me/DatosAbiertosGobAr'>canal de Datos Abiertos</a>,\n"
             "pronto podrás suscribirte para recibir notificaciones personalizadas 🔔 "
             "por dataset 📊 o temática 🗂️ a través de los comandos 'suscribir_dataset' y 'suscribir_tema'",
        parse_mode='HTML'
    )

async def temas_disponibles (update: Update, context: ContextTypes.DEFAULT_TYPE):
    formatted_themes = "\n".join([f"• {theme}" for theme in superthemes])
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"📚 Temas disponibles:\n\n{formatted_themes}",
        parse_mode="MarkdownV2"
    )

async def suscribir_tema(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_chat.id
    intento_tema =' '.join(context.args)
    if valid_theme(intento_tema,superthemes):
        if guardar_suscripcion_tema(user_id, intento_tema):
            await context.bot.send_message(chat_id=user_id, text="¡Tema válido! Recibirás novedades cuando hayan nuevos datasets"
                                                                 "o recursos de tu tema elegido.")
        else:
            await context.bot.send_message(chat_id=user_id,
                                           text="Hubo un problema y no se pudo guardar tu suscripción, es posible que ya estés suscripto a este tema 👀. Si no es así, intentálo más tarde.")
    else:
        await context.bot.send_message(chat_id=user_id,
                                       text= "¡Tema inválido! Podés consultar los temas disponibles con el comando '/temas_disponibles'.")


async def suscribir_dataset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_chat.id
    url_dataset =' '.join(context.args)
    if valid_dataset(url_dataset):
        dataset = extract_dataset_from_url(url_dataset)
        av_datasets_list = [v for k,v in av_datasets.items()]
        if dataset and dataset in av_datasets_list:
            if guardar_suscripcion_dataset(user_id, dataset):
                await context.bot.send_message(chat_id=user_id, text="¡Dataset válido! Recibirás una notificación cuando tenga actualizaciones.")
            else:
                await context.bot.send_message(chat_id=user_id, text="Hubo un problema para guardar tu suscripción, es posible que ya estés suscripto a este dataset 👀. Si no es así, intentálo más tarde.")

        else:
            await context.bot.send_message(chat_id=user_id, text="Dataset no válido, debe ser un dataset presente en datos.gob.ar.")

    else:
        await context.bot.send_message(chat_id=user_id,
                                       text="¡URL no válida!, debe empezar por datos.gob.ar/dataset/.")


if __name__ == '__main__':
    engine = create_engine(database_url, echo=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    application = ApplicationBuilder().token(bot_token).build()
    start_handler = CommandHandler('start', start)
    tema_handler = CommandHandler('suscribir_tema',suscribir_tema)
    tema_disp_handler = CommandHandler('temas_disponibles',temas_disponibles)
    dataset_handler = CommandHandler('suscribir_dataset',suscribir_dataset)
    application.add_handler(start_handler)
    application.add_handler(tema_disp_handler)
    application.add_handler(tema_handler)
    application.add_handler(dataset_handler)
    application.run_polling()
