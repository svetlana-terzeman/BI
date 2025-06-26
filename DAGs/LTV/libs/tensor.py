import time
from multiprocessing import Pool, cpu_count
import pandas as pd
import numpy as np
from functools import partial
from tqdm import tqdm
from libs.label_encoder      import MultiFeatureLabelEncoder

class Parallels3DCube:
    
        # Преобразование временных рядов в трехмерный массив для каждого пользователя
    def create_lstm_data(self, df, sequence_length, features:list, target:str, groupb_columns):
        lstm_data = []


        Xs, ys = [], []
        df_list = pd.DataFrame()
        for user, user_data in tqdm(df.groupby(groupb_columns)):
            
            for i in range(len(user_data)): # отбираем все записи (заказы) по client_id   
                # if i < len(user_data)-1: # записываем только с максимальной историей
                #     continue # прерывание итерации и переход к следующей
                if i >= sequence_length: # нарезаем тензоры до момента первой заполненности
                    continue # потом пропускаем все итерации
                
                
                user_data = user_data.sort_values(by = ['TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True, True, False, True])
                # Создаем пустой массив, который заполним записями
                basis       = np.full((sequence_length, len(features)), fill_value=-1, dtype=float) # шаблон для заполнения   # float  -1
                
                idx_col     = user_data.columns.get_indexer(features) # индексы столбцов в датафрейме
      
                target_col  = user_data.columns.get_indexer([target])
               
                # делаем срез в sequence_length двигаясь по каждой строчки датафрейма, чтобы нарезать на каждое поколение тензора
                fill_data     = user_data.iloc[0:i+1, idx_col].head(sequence_length).sort_index(ascending=True).values # заполняем только размером полученного фрейма был False (вверху тензора первые, потом остальые)

                target_data   = user_data.iloc[i, target_col].values
             
                basis[0:fill_data.shape[0], :] = fill_data # на каждую строку ерем ltv
                
                #return False
                
                Xs.append(basis)
                ys.append(target_data)
                
                # записываем по юзеру данные только с максимальной ситории (последние N записей)
                df_list = pd.concat([df_list, pd.DataFrame({'CUSTOMER_ID':user,'order_num':len(user_data), 'target_data':target_data, 'LIFETIMEDAY':user_data['LIFETIME_DAY'].max(), 'CASSTICKID_LAST':user_data['CASSTICKID'].tail(1).values[0], 'generation':i}, index=[0])])
              



        
        X_train, y_train  = np.array(Xs), np.array(ys)   
   
        return  X_train, y_train, df_list
    

    # функция которая работает с отобранной на ядро группой
    def apply_hampel_to_group(self, group:pd.DataFrame(), sequence_length:int, features:list, target:str, groupb_columns:list , pair_list:list):


        group                        =  group[group['CUSTOMER_ID'].isin(pair_list)] # обирается выборка по доступным на ядро наборам
        #print('apply_hampel_to_group start')
       
       
        

        # Создание трехмерного ряда данных LSTM для каждого пользователя
        group                        = group.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True])
   
        X_train, y_train,df_list     = self.create_lstm_data(group, sequence_length, features, target , groupb_columns)
       
        #print(y_train)
        return  X_train, y_train, df_list

    # функция которая формирует параллельные фичисления
    
    def parallelize_pairs(self, pair_list, sample, n_cores=1, sequence_length=10, features = ['price'], target = 'LTV' , groupb_columns=['CUSTOMER_ID']):
        pair_list_split = np.array_split(pair_list, n_cores) # уникальное кол-во клиентов разбивается на доступные ядра

        pool            = Pool(n_cores) # объявляется класс мультипроцесиснга
        #print('parallelize_pairs start')
        partial_func    = partial(self.apply_hampel_to_group, sample, sequence_length, features, target, groupb_columns)  # Частично применяем первый аргумент
        
        results         = pool.map(partial_func, pair_list_split) # применение функции со вторым аргументом, собираются в единый датафрейм результаты с разных ядре
        
        pool.close()
        pool.join()
      
        # конкантенируем все записи массивов       
        X = np.concatenate([result[0] for result in results], axis=0)
        y = np.concatenate([result[1] for result in results], axis=0)
        df_list = pd.concat([result[2] for result in results], axis=0)

        return X,y, df_list
    
    
    
class Pool3Dcube:
    
    def create_cube_array(self, data: pd.DataFrame, features: list, target: str, sequence_length: int):

        total_array = np.empty((0, sequence_length, len(features)))
        
        for customer_id, customer_data in tqdm(data.groupby('CUSTOMER_ID')):
            
            
            total_customer = np.empty((0, sequence_length, len(features)))
            customer_data = customer_data[features].reset_index(drop = True)
            customer_array = customer_data[-sequence_length:].values[::-1]
            if customer_array.shape[0] < sequence_length:
                # Создание массива со значениями -1
                additional_cols = np.full((sequence_length-customer_array.shape[0], len(features)), -1, dtype =float)
                # объединение массивов
                customer_array = np.concatenate((customer_array, additional_cols))
            # создаем трехмерный массив
            customer_array = np.reshape(customer_array, (1, customer_array.shape[0], customer_array.shape[1]))
            total_array = np.concatenate((total_array, customer_array))

        size = data['CUSTOMER_ID'].nunique()
        y = np.array(data.groupby('CUSTOMER_ID')[target].first().values).reshape(size, 1)

        return total_array, y
    
    
    def current_group(self, sample, sequence_length: int, features: list, target: str, customers_list: list):

        group = sample[sample['CUSTOMER_ID'].isin(customers_list)]

        X, y = self.create_cube_array(data = group, features = features, target = target, sequence_length = sequence_length)

        return X, y

    def parallelize_pairs(self, data: pd.DataFrame, store_clients_array, n_cores, sequence_length: int, features: list, target:str):

        splitted_data = np.array_split(store_clients_array, n_cores)

        pool = Pool(n_cores)

        partial_func = partial(self.current_group, data, sequence_length, features, target)

        results = pool.map(partial_func, splitted_data)

        pool.close()
        pool.join()


        X = np.concatenate([result[0] for result in results], axis = 0)
        y = np.concatenate([result[1] for result in results], axis = 0)

        return X, y