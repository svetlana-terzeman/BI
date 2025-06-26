import pandas as pd
import numpy as np
from libs.label_encoder      import MultiFeatureLabelEncoder

class PreprocessedData():
    
    def __init__(self):
        pass
    
    # Отбираем клиентов с первым заказом в выборке
    def FilterFirstOrder(self, df, distance_day = 0):
        
        print('FilterFirstOrder start')
        
        # Группируем по client_id и находим минимальную дату заказа для каждого клиента
        min_order_dates = df.groupby('CUSTOMER_ID')['TRADE_DT'].min().reset_index()

        # Объединяем с оригинальным датафреймом для получения даты первого заказа
        merged_df                   = pd.merge(min_order_dates, df[['CUSTOMER_ID', 'FIRSTORDERDATE']].drop_duplicates(), on='CUSTOMER_ID')
        merged_df['FIRSTORDERDATE'] = pd.to_datetime(merged_df['FIRSTORDERDATE'])
        merged_df['TRADE_DT']       = pd.to_datetime(merged_df['FIRSTORDERDATE'])
        
        # Отбираем клиентов, у которых минимальная дата заказа совпадает с датой первого заказа
        if distance_day == 0:
            filtered_clients = merged_df[merged_df['TRADE_DT'] == merged_df['FIRSTORDERDATE']]['CUSTOMER_ID'].unique()
        else:
            '''
            Дата заказа у нас считается по факту оплаты, а дата первого заказа по факту создания заказа. Поэтому у одного и того же заказа 2 отличающиеся даты
            '''
            # пытаемся это учесть, чтобы не исключать так много клиентов
            merged_df['distance_day']  = np.abs((merged_df['FIRSTORDERDATE'] - merged_df['TRADE_DT']).dt.days) 
            filtered_clients           = merged_df[merged_df['distance_day'] < distance_day]['CUSTOMER_ID'].unique()
            

        num_data = df[~df['CUSTOMER_ID'].isin(filtered_clients)]['CUSTOMER_ID'].nunique() 
        print(f'Кол-во клиентов не подлежат анализу: {num_data} из за проблем с датой первого заказа')


        # отбираем только клиентов у которых в преиод исследование начало клиентского пути (с первого заказа)
        df = df[df['CUSTOMER_ID'].isin(filtered_clients)]
        
        
        print('FilterFirstOrder finish')
        print()
        
        return df
    
    # Отбираем клиентов у которых первый заказ ONLINE
    def FilterFirstOrderOnline(self, df):
        
        print('FilterFirstOrderOnline start')
        
        dd = df[[
            'CUSTOMER_ID',
            'CASSTICKID',
            'TRADE_DT',
            'IDENTIFICATION',
            'FIRSTORDERDATE',
            'IDENTIFICATION_INDEX'

        ]].sort_values(by=['CUSTOMER_ID','TRADE_DT','IDENTIFICATION_INDEX'] , ascending=[True,True,True])


        first_orders = dd.sort_values(by=['CUSTOMER_ID','TRADE_DT','IDENTIFICATION_INDEX'], ascending=[True,True,True]).groupby('CUSTOMER_ID').apply(lambda x: x.iloc[0]).reset_index(drop=True)

        print('Кол-во клиентов с первым заказом Offline: ',first_orders[first_orders['IDENTIFICATION'] == 'OFFLINE']['CUSTOMER_ID'].nunique())

        # убираем клиентов у кого первый заказ в выборке OFFLINE
        online_first_clients = first_orders[first_orders['IDENTIFICATION'] == 'ONLINE']['CUSTOMER_ID'].unique()
        print('Кол-во клиентов с первым заказом Online: ', len(online_first_clients))


        df = df[df['CUSTOMER_ID'].isin(online_first_clients)]
        
        # Проверить наличие последующих оффлайн-заказов у этих клиентов
        num_data =df[(df['CUSTOMER_ID'].isin(online_first_clients)) & (df['IDENTIFICATION'] == 'OFFLINE')]['CUSTOMER_ID'].nunique()
        print(f'Кол-во клиентов у которых в последующих заказах есть OFFLINE: {num_data} из за проблем с датой первого заказа')
        
        print('FilterFirstOrderOnline finish')
        print()
        
        return df
        
        
    # Отбираем клиентов у которых в первом заказе заполнена FirstOrderDate
    def FilterFirstOrderDateEmpty(self, df):
        
        print('FilterFirstOrderDateEmpty start')
        
        
        # отбираем клиентов у которых первый статус пустой FirstOrderDate
        df_online_start  = df.sort_values(by=['CUSTOMER_ID','TRADE_DT','IDENTIFICATION_INDEX'] ,ascending=[True,True,True]).groupby(['CUSTOMER_ID'])[['CUSTOMER_ID','TRADE_DT','FIRSTORDERDATE','IDENTIFICATION']].apply(lambda x: x.iloc[0]).reset_index(drop=True)

        df_online_start = df_online_start[df_online_start['FIRSTORDERDATE'].isna()]['CUSTOMER_ID'].unique()

        # таких вообще не должно быть, так как ONLINE  заполняется всегда а мы отбираем всех кто с ONLINE начинает свой путь, но на всякий случай перепрвоеряем
        print(f'Кол-во клиентов у которых первый заказ пустой FirstOrderDate: {len(df_online_start)} ') 
    
        # оставляем с непустой первой датой
        df = df[~df['CUSTOMER_ID'].isin(df_online_start)]
        
        
        print('FilterFirstOrderDateEmpty finish')
        print()
        
        return df
        
    #  Отбираем клиентов у которых пропуски в ключевых полях
    def FilterKeyfiledsData(self, df, fields=['PRODUCT_CODE'], verbose=False):
        
        print('FilterKeyfiledsData start')
        
        for field in fields:
            print(f'field name:{field}')
            upsent_data = df[df[field].isna()]['CUSTOMER_ID'].unique()
            print(f'Кол-во клиентов у которых ключевые поля в заказх пустые: {len(upsent_data)} ') 
            
            if verbose:
                print(field)
                print(upsent_data[0:5])
            # исключаем таких клиентов
            df = df[~df['CUSTOMER_ID'].isin(upsent_data)]
           
        print('FilterKeyfiledsData finish')
        print()
        
        return df
    
    # Отбираем клиентов у которых более два заказа в 1 день
    def FilterManyOrders(self, df, num_orders = 2):
        
        print('FilterManyOrders start')
        
        #у скольки клиентов сколько заказов в 1 день
        filtered_clients = df.groupby(['CUSTOMER_ID','TRADE_DT'])['CASSTICKID'].nunique()

        # убираем клиентов из анализа которые покупали в один и тот же день более 2 покупок
        filtered_clients = filtered_clients[filtered_clients > num_orders].reset_index()['CUSTOMER_ID'].unique()
        print(f'Кол-во клиентов с заказами более {num_orders} в один день', len(filtered_clients))

        df = df[~(df['CUSTOMER_ID'].isin(filtered_clients))]
        
        print('FilterManyOrders finish')
        print()
        
        return df
    
    # заполняем пропуски в FIRSTORDERDATE так как в offline они не заполнены
    def FullingEmptyData(self, df, fields=['FIRSTORDERDATE']):
        
        print('FullingEmptyData start')

        # df.loc[:, fields] = df.groupby('CUSTOMER_ID')[fields].apply(lambda x: x.ffill().bfill()) # почему то перестал отрабатывать код

        df[fields] = df[fields].apply(lambda col: pd.to_datetime(col, errors='coerce'))

        df.loc[:, fields] = df.groupby('CUSTOMER_ID')[fields].transform(lambda x: x.ffill().bfill())

        print('FullingEmptyData finish')
        print()

        return df