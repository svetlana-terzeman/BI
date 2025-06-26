import numpy      as np
import pandas     as pd
from settings.constants import _correct_coef
from libs.label_encoder      import MultiFeatureLabelEncoder

def numpy_array_memory_usage_mb(arr: np.ndarray) -> float:
    # Получаем размер массива в байтах
    memory_usage_bytes = arr.nbytes
    
    # Переводим байты в мегабайты
    memory_usage_mb = memory_usage_bytes / (1024 ** 2)
    
    return memory_usage_mb

def merge_model_predictions(passport_of_models: list[dict]) -> pd.DataFrame:
    """Объединяет предсказания по всем моделям в один DataFrame.
    
    Args:
        passport_of_models (list[dict]): Список словарей с данными моделей.
        
    Returns:
        pd.DataFrame: Итоговый DataFrame с прогнозами всех моделей.
    """
    total_df = pd.DataFrame()
    
    for model_id in passport_of_models:
        sample                       = pd.DataFrame(model_id['data'])
        
        df_model                     = sample[sample['order_num'].between(*model_id['sequence_number'])].copy()
        print(df_model.shape)
        df_model['PREDICT_REVISION'] = model_id['revision']
        df_model['PREDICT_FEATURE']  = model_id['type_of_model']
        df_model['PREDICT_VALUE']    = model_id['prediction']
        df_model['TRESHOLD']         = model_id['optimal_treshold']

        df_model = df_model.rename(columns={'order_num': 'SEQUENCE_NUMBER', 'LIFETIMEDAY': 'CUSTOMER_LIFETIMEDAY'})
        df_model = df_model.drop(columns=['target_data'])
        
        total_df = pd.concat([total_df, df_model])

    # добавляем 1, так как с нуля начинается а нам нужно сравнивать с SEQUENCE_NUMBER где с 1 
    total_df['generation'] = total_df['generation'] + 1 
    #берем только актуальную запись
    total_df               = total_df.sort_values(by=['CUSTOMER_ID','generation'],ascending=[True,False]).groupby(['CUSTOMER_ID','PREDICT_REVISION']).head(1)
        
    return total_df

def transform_total_df(
                                total_df: pd.DataFrame,
                                df_base: pd.DataFrame
                            ) -> pd.DataFrame:
    """Для каждого таргета создает свою колонку

    Args:
        total_df (pd.DataFrame): DataFrame с прогнозами моделей.
        df_base (pd.DataFrame): Исходные данные с заказами клиентов.

    Returns:
        pd.DataFrame: Итоговый датасет с LTV, онлайн-долей и фактами.
    """
    group_files = ['CUSTOMER_ID','SEQUENCE_NUMBER','CUSTOMER_LIFETIMEDAY','CASSTICKID_LAST','generation']
    
    df_ltv          = total_df[total_df['PREDICT_FEATURE'] == 'LTV']
    df_ltv_online   = total_df[total_df['PREDICT_FEATURE'] == 'LTV_ONLINE_OFFLINE_FRACTION']
    
    max_tensor_size = df_ltv['generation'].max()
    print(f"max_tensor_size {max_tensor_size}")
    
    df_ltv        = df_ltv.set_index(group_files)
    df_ltv        = df_ltv.rename(columns={'PREDICT_VALUE':'ltv','PREDICT_REVISION':'PREDICT_REVISION_LTV', 'PREDICT_FEATURE':'PREDICT_FEATURE_LTV'})
    
    df_ltv_online = df_ltv_online.set_index(group_files)
    df_ltv_online = df_ltv_online.rename(columns={'PREDICT_VALUE':'ltv_online_frt','PREDICT_REVISION':'PREDICT_REVISION_LTV_ONLINE', 'PREDICT_FEATURE':'PREDICT_FEATURE_LTV_ONLINE'})
    
    db_result      = pd.concat([df_ltv, df_ltv_online],axis=1).reset_index()
    db_result      = db_result.set_index('CUSTOMER_ID')
    
    # считаем последовательные заказы
    df_ltv_current       = df_base.copy()
    df_ltv_current['SN'] = df_ltv_current.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True])\
                                         .groupby(['CUSTOMER_ID']).cumcount() + 1
    
    # отбираем заказ размера тензора по котрому строится предсказание
    df_ltv_current       = df_ltv_current[df_ltv_current['SN'] <= max_tensor_size].groupby(['CUSTOMER_ID'])['PRICEsum'].sum().reset_index()
    df_ltv_current       = df_ltv_current.set_index('CUSTOMER_ID')
    
    df_ltv_current       = df_ltv_current.rename(columns={'PRICEsum':'cumsum_fact'})
    
    db_result            = pd.concat([db_result, df_ltv_current],axis=1).reset_index()
    
    db_result[['ltv','ltv_online_frt','cumsum_fact']] = db_result[['ltv','ltv_online_frt','cumsum_fact']].astype(float)
    
    db_result['ltv']          = db_result.apply(lambda x:   x['cumsum_fact'] +  x['cumsum_fact'] * _correct_coef if x['ltv'] < x['cumsum_fact'] else x['ltv'], axis = 1)
    db_result['ltv_online']   = db_result['ltv'] * db_result['ltv_online_frt']

    df_ltv_correct = db_result[group_files + ['PREDICT_REVISION_LTV', 'PREDICT_FEATURE_LTV','ltv']].copy()
    df_ltv_correct = df_ltv_correct.rename(columns={'PREDICT_REVISION_LTV':'PREDICT_REVISION', 'PREDICT_FEATURE_LTV':'PREDICT_FEATURE', 'ltv':'PREDICT_VALUE'})
    df_ltv_correct['PREDICT_FEATURE'] = df_ltv_correct['PREDICT_FEATURE'].apply(lambda x: x + str('_PROD')) 
    
    df_ltv_online_correct                    = db_result[ group_files + ['PREDICT_REVISION_LTV_ONLINE', 'PREDICT_FEATURE_LTV_ONLINE','ltv_online']].copy()
    df_ltv_online_correct                    = df_ltv_online_correct.rename(columns={'PREDICT_REVISION_LTV_ONLINE':'PREDICT_REVISION', 'PREDICT_FEATURE_LTV_ONLINE':'PREDICT_FEATURE', 'ltv_online':'PREDICT_VALUE'})
    df_ltv_online_correct['PREDICT_FEATURE'] = df_ltv_online_correct['PREDICT_FEATURE'].apply(lambda x: x.replace('_FRACTION','') + str('_PROD')) 

    total_df         = pd.concat([total_df, df_ltv_correct, df_ltv_online_correct], axis=0)
    total_df.columns = total_df.columns.str.upper()
    
    return total_df

def add_lal_predictions(total_df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет и преобразует прогнозы LAL (Lifetime Activity Level) в общий DataFrame.

    Функция:
    1. Выбирает прогнозы LAL из общего DataFrame
    2. Бинаризует значения PREDICT_VALUE по порогу TRESHOLD
    3. Переименовывает признак PREDICT_FEATURE, добавляя '_PROD'
    4. Объединяет обратно с общими данными
    5. Удаляет столбец TRESHOLD

    Args:
        total_df (pd.DataFrame): Исходный DataFrame с прогнозами моделей

    Returns:
        pd.DataFrame: Модифицированный DataFrame с добавленными прогнозами LAL
    """
    # Выбираем только LAL прогнозы
    df_lal = total_df[total_df['PREDICT_FEATURE'] == 'LAL'].copy()
    
    # Бинаризуем значения по порогу
    df_lal['PREDICT_VALUE'] = df_lal.apply(
        lambda x: 1 if x['PREDICT_VALUE'] >= x['TRESHOLD'] else 0, 
        axis=1
    )
    
    # Переименовываем признак
    df_lal['PREDICT_FEATURE'] = df_lal['PREDICT_FEATURE'] + '_PROD'
    
    # Объединяем с исходными данными
    result_df = pd.concat([total_df, df_lal], axis=0)
    
    # Удаляем ненужный столбец
    result_df = result_df.drop(columns=['TRESHOLD'])
    
    return result_df