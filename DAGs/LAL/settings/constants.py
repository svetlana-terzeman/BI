import os
import pandas     as pd
import numpy      as np
from datetime import datetime
from dotenv   import load_dotenv
load_dotenv()  # по умолчанию ищет .env в текущей директории

# IP       = os.getenv('IP', '83.239.206.206')
# PORT     = int(os.getenv('PORT', 8011))          # HTTP-порт (8123 в контейнере)
# DB       = os.getenv('DB', 'ASH')
# # USERNAME = os.getenv('USERNAME', 'user6525')
# # PASSWD   = os.getenv('PASSWD', '34SwkLaaEi7aHfj0')
# USERNAME = os.getenv('USERNAME', 'default')
# PASSWD   = os.getenv('PASSWD', 'W9DjEWe78Jmy6VcR')


# REVISION        = os.getenv('REVISION', 'v20250522') # внедрены от этой даты новые модели
# reserach_period = int(os.getenv('reserach_period', 90))
# _correct_coef   =  os.getenv('_correct_coef', 0.2) # корректирующий коэфф. отклонения между факт.значением накопленным за N поколений в пеиод исследования и предсказания
# table_predict   = os.getenv('table_predict', 'BIG_DATA_LTV_ONLINE_OFFLINE_PREDICT')


IP       = '83.239.206.206'
PORT     =  8011
DB       = 'ASH'
USERNAME = 'user6525'
PASSWD   = '34SwkLaaEi7aHfj0'

HOST_BI     = 'db'
DB_BI       = 'BI'
USERNAME_BI = 'bitask_user'
PASSWD_BI   = 'bitask_pass'
tablename   = 'bi__task_register'

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_USER = 'admin'
RABBITMQ_PASS = 'mq_pass'
queue_name    = 'DAGs/LAL'
SCHEDULE_INTERVAL = 20 #время между проверками очереди в секундах
MAX_PARALLEL_TASKS = 1 #кол-во параллельных запусков

REVISION        = 'v20250522'
reserach_period = 90
_correct_coef   = 0.2
table_predict   = 'BIG_DATA_LTV_ONLINE_OFFLINE_PREDICT'
save_result     = ['db', 'parquet', 'excel'] # сохранение результата в бд 'db', в файл 'parquet', 'csv', 'excel'



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




