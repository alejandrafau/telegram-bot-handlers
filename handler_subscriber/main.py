import re
import os
import yaml
from telegram import Update,Bot
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
from dotenv import load_dotenv
from validators import valid_theme,valid_dataset,extract_dataset_from_url, valid_nodo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base,SuscripcionTema,SuscripcionDataset,SuscripcionNodo
import logging

load_dotenv()
bot_token = os.getenv("BOT_TOKEN")
Bot(bot_token).get_updates(offset=-1)
database_url = os.getenv("DATABASE_URL")
base_dir = os.path.dirname(os.path.abspath(__file__))
persistance_directory = os.path.join(base_dir, "..", "assets")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# -------------------------------
# FUNCIONES PARA CARGAR PERSISTENCIA
# -------------------------------
def load_superthemes():
    supertheme_path = os.path.join(persistance_directory, "superthemes.yaml")
    with open(supertheme_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_datasets():
    dataset_path = os.path.join(persistance_directory, "datasets.yaml")
    with open(dataset_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_organization():
    organization_path = os.path.join(persistance_directory, "organizations.yaml")
    with open(organization_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def escape_markdown(text: str) -> str:

    return re.sub(r'([_\-\.\!\*\#\+\=\|\{\}\~\>\(\)\[\]])', r'\\\1', text)

# -------------------------------
# FUNCIONES PARA BASE DE DATOS
# -------------------------------
async def guardar_suscripcion_nodo(user_id: int, nodo: str):
    db = SessionLocal()
    try:
        suscripcion = SuscripcionNodo(user_id=user_id, nodo=nodo)
        db.add(suscripcion)
        db.commit()
        db.refresh(suscripcion)
        return suscripcion
    except Exception as e:
        db.rollback()
        print("Error guardando suscripción nodo:", e)
    finally:
        db.close()


async def eliminar_suscripcion_tema_base(user_id: int, tema: str) -> bool:
    db = SessionLocal()
    try:
        suscripcion = (
            db.query(SuscripcionTema)
            .filter_by(user_id=user_id, tema=tema)
            .first()
        )
        logger.info(suscripcion)
        if suscripcion:
            db.delete(suscripcion)
            db.commit()
            return True
        return False
    except Exception as e:
        db.rollback()
        print("Error eliminando suscripción tema:", e)
        return False
    finally:
        db.close()


async def eliminar_suscripcion_dataset_base(user_id: int, dataset: str) -> bool:
    av_datasets = load_datasets()

    db = SessionLocal()
    try:
        suscripcion = (
            db.query(SuscripcionDataset)
            .filter_by(user_id=user_id, dataset=dataset)
            .first()
        )
        if suscripcion:
            db.delete(suscripcion)
            db.commit()
            return True
        return False
    except Exception as e:
        db.rollback()
        print("Error eliminando suscripción dataset:", e)
        return False
    finally:
        db.close()


async def eliminar_suscripcion_nodo_base(user_id: int, nodo: str) -> bool:
    db = SessionLocal()
    try:
        suscripcion = (
            db.query(SuscripcionNodo)
            .filter_by(user_id=user_id, nodo=nodo)
            .first()
        )
        if suscripcion:
            db.delete(suscripcion)
            db.commit()
            return True
        return False
    except Exception as e:
        db.rollback()
        print("Error eliminando suscripción nodo:", e)
        return False
    finally:
        db.close()


async def eliminar_todas_suscripciones_base(user_id: int):
    db = SessionLocal()
    try:
        db.query(SuscripcionTema).filter_by(user_id=user_id).delete()
        db.query(SuscripcionDataset).filter_by(user_id=user_id).delete()
        db.query(SuscripcionNodo).filter_by(user_id=user_id).delete()
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"Error eliminando las suscripciones de {user_id}:{e}")
        return False
    finally:
        db.close()

async def obtener_suscripciones_usuario(user_id: int) -> dict:
    db = SessionLocal()
    try:
        temas = [s.tema for s in db.query(SuscripcionTema).filter_by(user_id=user_id).all()]
        datasets = [s.dataset for s in db.query(SuscripcionDataset).filter_by(user_id=user_id).all()]
        nodos = [s.nodo for s in db.query(SuscripcionNodo).filter_by(user_id=user_id).all()]

        return {
            "temas": temas,
            "datasets": datasets,
            "nodos": nodos
        }
    finally:
        db.close()
async def guardar_suscripcion_tema(user_id: int, tema: str):
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

async def guardar_suscripcion_dataset(user_id: int, dataset: str):
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

# -------------------------------
# HANDLERS
# -------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(update.effective_chat.id)
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "¡Hola! Soy DATOB, te cuento con qué te puedo ayudar:\n\n"
            "📌 - Podés suscribirte a un tema de tu interés con /suscribir_tema. "
            "Para ver los temas disponibles podés usar /temas_disponibles. "
            "Así te enterarás de nuevos datasets y recursos vinculados a ese tema.\n\n"
            "🏛️ - Podés suscribirte a un nodo con /suscribir_nodo. "
            "Para ver los nodos disponibles podés usar /nodos_disponibles. "
            "Así te enterarás de nuevos datasets y recursos vinculados a ese organismo.\n\n"
            "💾 - Podés suscribirte a un dataset con /suscribir_dataset. "
            "Necesitás contar con una URL válida del mismo. Así recibirás notificaciones "
            "de nuevos recursos o datos que se le agreguen.\n\n"
            "🗑️ - Podés consultar o borrar todas tus suscripciones con /mis_suscripciones, "
            "/eliminar_nodo, /eliminar_tema, "
            "/eliminar_dataset o simplemente /eliminar_todas."
        ),
        parse_mode='HTML'
    )

async def temas_disponibles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    superthemes = load_superthemes()
    formatted_themes = "\n".join([
        f"• *{escape_markdown(key)}*: {escape_markdown(value)}"
        for key, value in superthemes.items()
    ])
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"📚 Temas disponibles:\n\n{formatted_themes}",
        parse_mode="MarkdownV2"
    )


async def suscribir_tema(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_chat.id
    intento_tema = ' '.join(context.args)
    supertheme_dict = load_superthemes()
    superthemes = list(supertheme_dict.keys())

    if valid_theme(intento_tema, superthemes):
        if await guardar_suscripcion_tema(user_id, intento_tema):
            await context.bot.send_message(
                chat_id=user_id,
                text="¡Tema válido! Recibirás novedades cuando hayan nuevos datasets "
                     "o recursos de tu tema elegido."
            )
        else:
            await context.bot.send_message(
                chat_id=user_id,
                text="Hubo un problema y no se pudo guardar tu suscripción, es posible que ya estés "
                     "suscripto a este tema 👀. Si no es así, intentálo más tarde."
            )
    else:
        await context.bot.send_message(
            chat_id=user_id,
            text="¡Tema inválido! Podés consultar los temas disponibles con el comando "
                 "'/temas_disponibles'. Usá la versión corta o alias del tema."
        )



async def suscribir_dataset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    av_datasets = load_datasets()
    user_id = update.effective_chat.id
    url_dataset =' '.join(context.args)
    if valid_dataset(url_dataset):
        dataset = extract_dataset_from_url(url_dataset)
        print(dataset)
        dataset_id = None
        for k,v in av_datasets.items():
            if v == dataset:
                dataset_id = k
                break

        if dataset_id:
            if await guardar_suscripcion_dataset(user_id, dataset_id):
                    await context.bot.send_message(chat_id=user_id, text="¡Dataset válido! Recibirás una notificación cuando tenga actualizaciones.")
            else:
                    await context.bot.send_message(chat_id=user_id, text="Hubo un problema para guardar tu suscripción, es posible que ya estés suscripto a este dataset 👀. Si no es así, intentálo más tarde.")

        else:
            await context.bot.send_message(chat_id=user_id, text="Dataset no encontrado")

    else:
        await context.bot.send_message(chat_id=user_id,
                                       text="¡URL no válida!, debe empezar por https://datos.gob.ar/dataset/.")


async def nodos_disponibles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    nodos = load_organization()
    formatted_nodos = "\n".join([
        f"• *{escape_markdown(key)}*: {escape_markdown(value)}"
        for key, value in nodos.items()
    ])

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"🛰️ Nodos disponibles:\n\n{formatted_nodos}",
        parse_mode="MarkdownV2"
    )


# Suscribir nodo
async def suscribir_nodo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("entró acá")
    user_id = update.effective_chat.id
    intento_nodo = " ".join(context.args)
    nodos = list(load_organization().keys())
    if valid_nodo(intento_nodo, nodos):
        if await guardar_suscripcion_nodo(user_id, intento_nodo):
            await context.bot.send_message(
                chat_id=user_id,
                text=f"✅ Suscripción a '{intento_nodo}' completada. Recibirás novedades de este nodo."
            )
        else:
            await context.bot.send_message(
                chat_id=user_id,
                text="⚠️ No se pudo guardar tu suscripción. Quizás ya estabas suscripto 👀."
            )
    else:
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ Nodo inválido. Usá el comando /nodos_disponibles para consultar la lista.Es necesario usar el nombre corto o alias"
        )


# Mis suscripciones (temas, datasets y nodos)
async def mis_suscripciones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_chat.id
    suscripciones = await obtener_suscripciones_usuario(user_id)

    msg = "📌 *Tus suscripciones actuales:*\n\n"
    msg += "📚 Temas:\n" + (
                "\n".join([f"• {escape_markdown(t)}" for t in suscripciones["temas"]]) or "– Ninguno") + "\n\n"
    msg += "📊 Datasets:\n" + (
                "\n".join([f"• {escape_markdown(d)}" for d in suscripciones["datasets"]]) or "– Ninguno") + "\n\n"
    msg += "🛰️ Nodos:\n" + ("\n".join([f"• {escape_markdown(n)}" for n in suscripciones["nodos"]]) or "– Ninguno")

    await context.bot.send_message(chat_id=user_id, text=msg, parse_mode="MarkdownV2")


# Eliminar suscripción nodo
async def eliminar_suscripcion_nodo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_chat.id
    nodo = " ".join(context.args)
    if await eliminar_suscripcion_nodo_base(user_id, nodo):
        msg = f"🗑️ Suscripción al nodo '{nodo}' eliminada."
    else:
        msg = f"⚠️ No se pudo eliminar suscripción a nodo: '{nodo}'."
    await context.bot.send_message(chat_id=user_id, text=msg)


# Eliminar suscripción tema
async def eliminar_suscripcion_tema(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_chat.id
    tema = " ".join(context.args)
    if await eliminar_suscripcion_tema_base(user_id, tema):
        msg = f"🗑️ Suscripción al tema '{tema}' eliminada."
    else:
        msg = f"⚠️ No se pudo eliminar tu suscripción al tema '{tema}'."
    await context.bot.send_message(chat_id=user_id, text=msg)


# Eliminar suscripción dataset
async def eliminar_suscripcion_dataset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_chat.id
    dataset = " ".join(context.args)
    if await eliminar_suscripcion_dataset_base(user_id, dataset):
        msg = f"🗑️ Suscripción al dataset '{dataset}' eliminada."
    else:
        msg = (f"⚠️ No se pudo eliminar tu suscripción al dataset {dataset}.Para eliminar"
               f"es necesario usar el id del dataset, lo podés consultar en /mis_suscripciones")
    await context.bot.send_message(chat_id=user_id, text=msg)


# Eliminar todas las suscripciones
async def eliminar_todas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_chat.id
    if await eliminar_todas_suscripciones_base(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="🧹 Todas tus suscripciones han sido eliminadas."
        )
    else:
        await context.bot.send_message(
            chat_id=user_id,
            text="Hubo un problema eliminando tu suscripciones."
        )
if __name__ == '__main__':
    engine = create_engine(database_url, echo=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    application = ApplicationBuilder().token(bot_token).build()
    start_handler = CommandHandler('start', start)
    tema_handler = CommandHandler('suscribir_tema',suscribir_tema)
    tema_disp_handler = CommandHandler('temas_disponibles',temas_disponibles)
    dataset_handler = CommandHandler('suscribir_dataset',suscribir_dataset)
    nodo_handler = CommandHandler('suscribir_nodo',suscribir_nodo)
    nodo_disp_handler = CommandHandler('nodos_disponibles',nodos_disponibles)
    misuscri_handler = CommandHandler('mis_suscripciones',mis_suscripciones)
    er_nodo_handler = CommandHandler('eliminar_nodo', eliminar_suscripcion_nodo)
    er_tema_handler = CommandHandler('eliminar_tema', eliminar_suscripcion_tema)
    er_dataset_handler = CommandHandler('eliminar_dataset', eliminar_suscripcion_dataset)
    er_all = CommandHandler('eliminar_todas', eliminar_todas)
    application.add_handler(start_handler)
    application.add_handler(tema_disp_handler)
    application.add_handler(tema_handler)
    application.add_handler(dataset_handler)
    application.add_handler(nodo_disp_handler)
    application.add_handler(nodo_handler)
    application.add_handler(misuscri_handler)
    application.add_handler(er_nodo_handler)
    application.add_handler(er_tema_handler)
    application.add_handler(er_dataset_handler)
    application.add_handler(er_all)
    application.run_polling()
