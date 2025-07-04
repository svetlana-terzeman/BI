import time
import os
import pika
import json
import logging
import warnings
import psycopg2
import pandas   as pd
from datetime           import datetime
from libs               import queries, run_models, functions, connection_db
from settings.constants import (passport_of_models, REVISION, reserach_period, save_result, \
                                HOST_BI, DB_BI, USERNAME_BI, PASSWD_BI, tablename,\
                                RABBITMQ_USER, RABBITMQ_PASS, RABBITMQ_HOST, queue_name, \
                                SCHEDULE_INTERVAL, MAX_PARALLEL_TASKS )
from libs.label_encoder              import MultiFeatureLabelEncoder
from apscheduler.schedulers.blocking import BlockingScheduler

warnings.filterwarnings("ignore")

# Настройка логов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scoring_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def rabbitMQ_connection():
    """Установка соединения с RabbitMQ"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters  = pika.ConnectionParameters(
                                                host=RABBITMQ_HOST,
                                                credentials=credentials,
                                                heartbeat=600,
                                                blocked_connection_timeout=300,
                                                connection_attempts=5, #кол-во попыток подключения
                                                retry_delay=10    #ол-во секунд между попытками
                                            )
    connection = pika.BlockingConnection(parameters)
    channel    = connection.channel()

    # Объявляем очереди
    channel.queue_declare(queue=queue_name, durable=True)

    logger.info("Подключение к RabbitMQ успешно установлено")

    return channel


def get_db_connection():
    return psycopg2.connect(
        dbname=DB_BI, user=USERNAME_BI,
        password=PASSWD_BI, host=HOST_BI
    )


def process_task(task_data, filename):
    """Основная логика обработки задачи"""
    try:
        start_time = time.time()
        logger.info(f"Начало обработки задачи: {task_data}")

        reserach_period = task_data.get('reserach_period', os.getenv("RESEARCH_PERIOD"))
        currdate = task_data.get('currdate', os.getenv("CURRDATE")) #pd.to_datetime(datetime.now()).date().strftime('%Y-%m-%d')

        # Загрузка данных из БД
        logger.info("Загрузка данных из БД...")
        query_base = queries.scoring_segment(reserach_period, currdate)
        df_base    = connection_db.QueryExecuted(query_base)
        logger.info(f"Загружено {len(df_base)} записей")

        # Обработка моделей
        logger.info("Запуск обработки моделей...")
        for model_data in passport_of_models:
            modeling = run_models.Deploy(model_data)
            model_data = modeling.main(df_base)
            logger.info(f"Модель отработала")

        # Формирование результатов
        logger.info("Формирование результатов...")
        total_df = functions.merge_model_predictions(passport_of_models)
        total_df = functions.transform_total_df(total_df, df_base)
        total_df = functions.add_lal_predictions(total_df)
        # Удаляем ненужный столбец
        total_df = total_df.drop(columns=['TRESHOLD'])
        total_df['REVISION'] = REVISION
        total_df['CUSTOMER_LIFETIMEDAY'] = total_df['CUSTOMER_LIFETIMEDAY'].astype(int)

        # Сохранение результатов
        logger.info("Сохранение результатов...")
        os.makedirs('/app/result', exist_ok=True)

        if 'db' in save_result:
            connection_db.save_to_clickhouse(total_df)
            logger.info("Результаты сохранены в БД")
        if 'csv' in save_result:
            csv_path = f'/app/result/{filename}.csv'
            total_df.to_csv(csv_path, index=False)
            logger.info(f"Результаты сохранены в CSV: {csv_path}")
        if 'excel' in save_result:
            excel_path = f'/app/result/{filename}.xlsx'
            total_df.to_excel(excel_path, index=False)
            logger.info(f"Результаты сохранены в Excel: {excel_path}")
        if 'parquet' in save_result:
            parquet_path = f'/app/result/{filename}.parquet'
            total_df.to_parquet(parquet_path, index=False)
            logger.info(f"Результаты сохранены в Parquet: {parquet_path}")

        execution_time = time.time() - start_time
        logger.info(f"Задача успешно выполнена за {execution_time:.2f} секунд")
        return True

    except Exception as e:
        logger.error(f"Ошибка при обработке задачи: {str(e)}", exc_info=True)
        return False



def on_message(channel, method, properties, body):
        conn    = get_db_connection()
        cursor  = conn.cursor()
        try:
            task    = json.loads(body)
            task_id = task.get('id')
            cursor.execute(f"UPDATE {tablename} SET status = 2 WHERE id = %s", (task_id,))
            conn.commit()
            logger.info(f"Получена задача ID: {task_id}")

            # Выполняем задачу
            success = process_task(task.get('metadata'), task.get('filename'))

            # Отправляем финальный статус
            status = 3 if success else 4
            cursor.execute(f"UPDATE {tablename} SET status = %s WHERE id = %s", (status, task_id))
            conn.commit()
            channel.basic_ack(delivery_tag=method.delivery_tag)



        except Exception as e:
            cursor.execute(f"UPDATE {tablename} SET status = 4 WHERE id = %s", (task_id, ))
            conn.commit()
            logger.error(f"Критическая ошибка обработки сообщения: {str(e)}", exc_info=True)

        conn.close()
        logger.info(f"Статус задачи {task_id} обновлен на {status}")



def main():
    try:
        channel = rabbitMQ_connection()

        # Настройка потребителя
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )

        logger.info("Сервис scoring_service запущен. Ожидание задач...")
        channel.start_consuming()

    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}", exc_info=True)



if __name__ == "__main__":
    try:
        scheduler = BlockingScheduler(timezone="Europe/Moscow")
        scheduler.add_job(main, 'interval', seconds=SCHEDULE_INTERVAL, max_instances=MAX_PARALLEL_TASKS)
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Сервис остановлен пользователем")

    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}", exc_info=True)