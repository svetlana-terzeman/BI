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
queue_name    = 'DAGs/LTV'
SCHEDULE_INTERVAL = 20 #время между проверками очереди в секундах
MAX_PARALLEL_TASKS = 1 #кол-во параллельных запусков

REVISION        = 'v20250522'
reserach_period = 90
_correct_coef   = 0.2
table_predict   = 'BIG_DATA_LTV_ONLINE_OFFLINE_PREDICT'
save_result     = ['db', 'parquet', 'excel'] # сохранение результата в бд 'db', в файл 'parquet', 'csv', 'excel'



passport_of_models = [




 # модель которая обучена на 3 тензора по данным 1-6 заказа с чутом сееплинга в 13000 примеров 1 заказа
 # выборка строилась слайсингом, по каждому лкиенту нарезалось от 1 до 3 примеров в заивисомтси от его поколения

    
    # экспериментируем (на семплинге) 
    # до 22052025 '_ltv__v31102024__dataslicing_1-3d_ten3__all1gen'
    {
        'revision':             '_ltv__v11052025__dataslicing_1-3d_ten3__all1gen', # с 22052025
        'type_of_model':        'LTV',
        'categorical_features':  [
                                        'MART_NAME_RUmost_frequent_category',
                                        'SEGMENT_NAME_RUmost_frequent_category', 
                                        'CATEGORY_NAME_RUmost_frequent_category', 
                                        'FAMILY_NAME_RUmost_frequent_category'
                                ],
        'sequence_number':       (1,3)
    },   
    
   # для заказов от 4  
   # до 22052025 _ltv__v31102024__dataslicing_4-10d_ten10__all1gen
   {
        'revision':             '_ltv__v11052025__dataslicing_4-10d_ten10__all1gen', # с 22052025
        'type_of_model':        'LTV',
        'categorical_features':  [
                                  'MART_NAME_RUmost_frequent_category',
                                  'SEGMENT_NAME_RUmost_frequent_category', 
                                  'CATEGORY_NAME_RUmost_frequent_category', 
                                  'FAMILY_NAME_RUmost_frequent_category'
        ],
        'sequence_number':       (4,10000)
    }, 
    
    
   
    
   
# модель которая предсказывает долю онлайн покупок из оффлайн, строиалсь на клиентах у которых минимум 3-6 заказов, так как у них есть оффлайн
# нет смысла брать клиентов у котрых 1 заказ так как у них все онлайн и два заказа так как там проактически нет офлайнов
# тензор так же поставили 3 чтобы делать предсказания на ранеей стадии
# до 22052025 _ltv_online_frt__v31102024__dataslicing_1-3d_ten3__all1gen
    {
        'revision':             '_ltv_online_frt__v11052025__dataslicing_1-3d_ten10__all1gen',  # с 22052025 
        'type_of_model':        'LTV_ONLINE_OFFLINE_FRACTION',
        'categorical_features':  [
                                 'MART_NAME_RUmost_frequent_category',
                                 'SEGMENT_NAME_RUmost_frequent_category', 
                                 'CATEGORY_NAME_RUmost_frequent_category', 
                                 'FAMILY_NAME_RUmost_frequent_category'
                                ],
        'sequence_number':(1,3)
    },   
    
    # применяется для заказов 4 и более
    # до 22052025 _ltv_online_frt__v31102024__dataslicing_4-10d_ten10__all1gen
    {
        'revision':             '_ltv_online_frt__v11052025__dataslicing_4-10d_ten10__all1gen',  # с 22052025 
        'type_of_model':        'LTV_ONLINE_OFFLINE_FRACTION',
        'categorical_features':  [
                                 'MART_NAME_RUmost_frequent_category',
                                 'SEGMENT_NAME_RUmost_frequent_category', 
                                 'CATEGORY_NAME_RUmost_frequent_category', 
                                 'FAMILY_NAME_RUmost_frequent_category'
                                ],
        'sequence_number':       (4,10000)
    },
    
]




