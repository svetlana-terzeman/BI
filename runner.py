import time
import pika
import psycopg2
import json
from settings.constants import USERNAME, PASSWD, HOST, DB, RABBITMQ_USER, RABBITMQ_PASS, RABBITMQ_HOST, tablename, scheme_forms
import logging
from libs.scheme_db import Base, engine
from apscheduler.schedulers.blocking import BlockingScheduler

# Base.metadata.create_all(bind=engine)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# подключение к БД BI
def get_db_connection():
    return psycopg2.connect(dbname=DB, user=USERNAME, password=PASSWD, host=HOST)

# подключение к RABBITMQ
def setup_rabbitmq_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    service_names = scheme_forms.keys()
    for service_name in service_names:
        channel.queue_declare(queue=f"DAGs/{service_name}", durable=True)
    return connection, channel


def runner():
    try:
        db_conn                    = get_db_connection()
        rabbit_conn, rabbit_channel = setup_rabbitmq_connection()
        cursor = db_conn.cursor()
        # выбираем задачи со статусом 0
        cursor.execute(f"SELECT id, dag_name, task_metadata, filename FROM {tablename} WHERE status = 0")
        tasks = cursor.fetchall()

        # отправляем все найденные задачи в очередь
        for task_id, service_name, data, filename in tasks:
            rabbit_channel.basic_publish(
                exchange    = '',
                routing_key = f"DAGs/{service_name}",
                body        = json.dumps({'id': task_id, 'metadata': data,'filename': filename}),
                properties  = pika.BasicProperties(delivery_mode=2)  # для устойчивости очереди
            )

            # изменение статуса на 1
            cursor.execute(f"UPDATE {tablename} SET status = 1 WHERE id = %s", (task_id,))
            db_conn.commit()

            logger.info(f"Задача {task_id} отправлена в очередь DAGs/{service_name}")

        cursor.close()

    except (psycopg2.OperationalError, pika.exceptions.AMQPError) as e:
        logger.error(f"Ошибка подключения: {str(e)}")

    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}")

if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone="Europe/Moscow")
    scheduler.add_job(runner, 'interval', seconds=5)

    try:
        scheduler.start()
        logger.info("Сервис запущен. Ожидание новых задач...")


    except (KeyboardInterrupt, SystemExit):
        logger.info("Сервис остановлен")

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        time.sleep(10)
