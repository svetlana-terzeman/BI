import os

HOST     =  os.getenv('DB_HOST', 'db')
DB       =  os.getenv('DB_NAME', 'BI')
USERNAME =  os.getenv('DB_USER', 'bitask_user')
PASSWD   =  os.getenv('DB_PASS', 'bitask_pass')
tablename =  os.getenv('BI_tablename', 'bi__task_register')
PORT      =  int(os.getenv('DB_PORT', 5432))

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_DEFAULT_USER', 'admin')
RABBITMQ_PASS = os.getenv('RABBITMQ_DEFAULT_PASS', 'mq_pass')

retry_delay   = int(os.getenv('retry_delay', 5))  # время ожидания подключения к RabbitMQ
max_retries   = int(os.getenv('max_retries', 15)) # максимальное кол-во попыток подключения к RabbitMQ

external_IP   = os.getenv('external_IP', None)
external_port = int(os.getenv('external_port', 8000))

scheme_forms = {'LTV': {
                            'CURRDATE': {
                                            'type'     : 'datetime',
                                            'required' : True
                                         },
                            'RESEARCH_PERIOD': {
                                            'type'     : 'int',
                                            'required' : True
                                         }

                        },
                'LAL': {
                            'CURRDATE': {
                                            'type'     : 'datetime',
                                            'required' : True
                                         },
                            'RESEARCH_PERIOD': {
                                            'type'     : 'int',
                                            'required' : True
                                         }

                        },
                # 'ChurnRate': {}

                }

status_name = {
                    0: 'добавлен',
                    1: 'в очереди',
                    2: 'на обработке',
                    3: 'успешно',
                    4: 'ошибка'
              }