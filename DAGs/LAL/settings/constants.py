import os
from dotenv   import load_dotenv
load_dotenv("/app/.env")

IP       = os.getenv('IP_DATA', '83.239.206.206')
PORT     = int(os.getenv('PORT_DATA', 8011))          # HTTP-порт (8123 в контейнере)
DB       = os.getenv('DB_DATA', 'ASH')
USERNAME = os.getenv('USERNAME_DATA', 'user6525')
PASSWD   = os.getenv('PASSWD_DATA', '34SwkLaaEi7aHfj0')

HOST_BI     =  os.getenv('DB_HOST', 'db')
DB_BI       =  os.getenv('DB_NAME', 'BI')
USERNAME_BI =  os.getenv('DB_USER', 'bitask_user')
PASSWD_BI   =  os.getenv('DB_PASS', 'bitask_pass')
tablename   =  os.getenv('BI_tablename', 'bi__task_register')

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'admin')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'mq_pass')
queue_name    = os.getenv('queue_name_LAL', 'DAGs/LAL')

SCHEDULE_INTERVAL  = int(os.getenv('SCHEDULE_INTERVAL', 20)) #время между проверками очереди в секундах
MAX_PARALLEL_TASKS = int(os.getenv('MAX_PARALLEL_TASKS', 1)) #кол-во параллельных запусков

REVISION        = os.getenv('REVISION', 'v20250522')
_correct_coef   = float(os.getenv('_correct_coef', 0.2))
table_predict   = os.getenv('table_predict', 'BIG_DATA_LTV_ONLINE_OFFLINE_PREDICT')
save_result     = os.getenv('save_result', ['db', 'parquet', 'excel'])  # сохранение результата в бд 'db', в файл 'parquet', 'csv', 'excel'


passport_of_models = [

   # применяется для заказов 1-6 поиска look-a-like клиентов у кого сумма заказа свыше 20 тыс. 
    # и до и с 22052025 остается та же для нее не считали еще пока что _LAL__v28102024__dataslicing_1-20d_ten6__all1gen
    {
        'revision':             '_LAL__v28102024__dataslicing_1-20d_ten6__all1gen',
        'type_of_model':        'LAL',
        'categorical_features':  [ # важно сохранить порядок фичей, так как он поадался в модель
                                        'MART_NAME_RUmost_frequent_category',
                                        'SEGMENT_NAME_RUmost_frequent_category', 
                                        'CATEGORY_NAME_RUmost_frequent_category', 
                                        'FAMILY_NAME_RUmost_frequent_category'
                                 ],
        'sequence_number':       (1,10000)
    }, 
    
]




