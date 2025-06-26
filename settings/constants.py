import os

HOST       = 'db'
PORT     =  5432
DB       = 'BI'
USERNAME = 'bitask_user'
PASSWD   = 'bitask_pass'
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_USER = 'admin'
RABBITMQ_PASS = 'mq_pass'
tablename     = 'bi__task_register'
retry_delay   = 5  # время ожидания подключения к RabbitMQ
max_retries   = 15 # максимальное кол-во попыток подключения к RabbitMQ

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

