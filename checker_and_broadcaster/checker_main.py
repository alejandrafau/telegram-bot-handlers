import parser as pars
import broadcast as broad
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import asyncio


sender_email = os.getenv("EMAIL_SENDER")
sender_password = os.getenv("GMAIL_PASS")
recipient_email = os.getenv("EMAIL_RECEIVERS")
database_url = os.getenv("DATABASE_URL")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

async def message_sending(broadcaster):
    try:
        await broadcaster.send_new_dataset_message()
        await broadcaster.send_new_distribution_message()
        await broadcaster.send_new_datapoint_message()
    except Exception as e:
        raise e



if __name__ == '__main__':
    db_engine = create_engine(database_url, echo=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)

try:
    # -------------------------------
    # PARSEO: Compara estado actual y anterior del portal incluyendo cambios en tamaño en
    #distribuciones de datasets con suscriptores
    # -------------------------------
    parser = pars.Parser(db_engine, SessionLocal)
    parser.dump_current_nodes()
    parser.dump_current_themes()
    parser.dump_current_datasets()
    parser.dump_error_report()
    parser.save_current_state()
    events = parser.serialize_events()

    # ------------------------------------------------------------------------
    # BROADCAST: Envía mensajes de acuerdo al evento tratando de no replicar.
    # Si un usuario está suscrito a un tema y a un nodo, y hay un dataset nuevo
    # que corresponde a ambos, el usuario recibirá la notificación sólo una vez
    # ------------------------------------------------------------------------

    broadcaster = broad.Broadcaster()
    broadcaster.db_engine = db_engine
    broadcaster.db_session = SessionLocal()
    broadcaster.set_events(events)
    asyncio.run(message_sending(broadcaster))
    pers_directory = broadcaster.persistance_directory
    error_path = os.path.join(pers_directory, "error_report.csv")
    message = "Datob parseó y mandó notificaciones correctamente"
    subject = "Reporte Datob (Parser y Broadcaster)"
    body = "\n\nDistribuciones que no se pudieron parsear\n\n"
    attachments = [error_path]

    for recipient in recipient_email.split(","):
        recipient = recipient.strip()
        broadcaster.send_email_report(
            sender_email=sender_email,
            sender_password=sender_password,
            recipient_email=recipient,
            subject=subject,
            body=message,
            attachment_paths=attachments
        )

except Exception as e:

    broadcaster = broad.Broadcaster()
    message = f"Ocurrió el siguiente error: {e}"
    subject = "Error - Reporte Datob (Parser y Broadcaster)"

    for recipient in recipient_email.split(","):
        recipient = recipient.strip()
        broadcaster.send_email_report(
            sender_email=sender_email,
            sender_password=sender_password,
            recipient_email=recipient,
            subject=subject,
            body=message
        )



