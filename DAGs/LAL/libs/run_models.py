# import os
# import pickle
# import time
#
# import pandas as pd
# import numpy as np
# # from tensorflow.keras.models import load_model
# from keras.models import load_model
# from sklearn.preprocessing   import OneHotEncoder, MinMaxScaler
# from libs.label_encoder      import MultiFeatureLabelEncoder
# from libs                    import tensor, functions

import pickle
import os
import time
import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
# from tensorflow.python.keras.models import load_model
from sklearn.preprocessing   import OneHotEncoder, MinMaxScaler

from libs.label_encoder      import MultiFeatureLabelEncoder
from libs                    import tensor, functions
import warnings
warnings.filterwarnings("ignore")

class Deploy: 
    def __init__(self, model_data):
        self.n_cores = 10
        self.model_data           = model_data
        self.model_embends        = False
        self.inverse_y            = False
        self.revision             = self.model_data['revision']
        self.type_of_model        = self.model_data['type_of_model']
        self.categorical_features = self.model_data['categorical_features']
        self.seq_nums             = self.model_data['sequence_number']
        encoder, scaler, self.X_scaler, self.y_scaler       = None,None,None,None
        
    def load_data(self):
        
        # Загрузка фичей из файла
        if os.path.exists(f'models/features{self.revision}.pkl'):
            with open(f'models/features{self.revision}.pkl', 'rb') as file:
                self.features         = pickle.load(file)
                self.features_list         = list(self.features.keys())
                print('Фичи загружены.')
        else:
            print('Фичи не найдены. Выход (!)')
            return 'Error'

        # Загружаем модели, гиперпараметры и метаданные
        if os.path.exists(f'models/model{self.revision}.keras'):
            self.model = load_model(f'models/model{self.revision}.keras')
            print('Модель загружена.')

        else:
            print('Не найдена модель. Выход (!)')
            return 'Error'

        # Загрузка метаданных из файла
        if os.path.exists(f'models/metadata{self.revision}.pkl'):
            with open(f'models/metadata{self.revision}.pkl', 'rb') as file:
                self.metadata = pickle.load(file)
                print('Метаданные загружены.')
        
         # Загрузка энкодера из файла
        if os.path.exists(f'models/onehot_encoder{self.revision}.pkl'):
            with open(f'models/onehot_encoder{self.revision}.pkl', 'rb') as file:
                self.encoder = pickle.load(file)
                print('Энкодер категоарильных фичей загружен.')

        # Загрузка энкодера из файла
        if os.path.exists(f'models/label_encoder{self.revision}.pkl'):
            with open(f'models/label_encoder{self.revision}.pkl', 'rb') as file:
                self.lab_encoder = pickle.load(file)
                print('Кодирование категоарильных фичей для эмбендингов загружен.')
        
        # Загрузка скейлера из файла
        if os.path.exists(f'models/scalers{self.revision}.pkl'):
            with open(f'models/scalers{self.revision}.pkl', 'rb') as file:
                self.scaler = pickle.load(file)
                # если несколько скейлеров представлено для X и Y
                if isinstance(self.scaler, dict) and len(self.scaler)  >  0: 
                    self.X_scaler = self.scaler['X']
                    self.y_scaler = self.scaler['y']
                    print('загружены X и y скейлеры.')               
                print('Нормализатор загружен.') 
    def preproc(self, data):
         # если в паспорте есть энкодер, то применяем его
        if hasattr(self, 'encoder') and isinstance(self.encoder, OneHotEncoder):
            # кодируем категориальыне фичи
            if len(self.categorical_features) > 0:
                df_encoder  = self.encoder.transform(data[self.categorical_features]) # применяем
                df_encoder  = pd.DataFrame(df_encoder, columns=self.encoder.get_feature_names_out(self.categorical_features))
                data        = pd.concat([data.drop(self.categorical_features, axis=1, errors='ignore'),  df_encoder], axis=1)
                print('Былы примененя категоризация переменных.')
    
    
        # если был применен кодировщик для эмбендингов
        if hasattr(self, 'lab_encoder') and isinstance(self.lab_encoder, MultiFeatureLabelEncoder):
            # кодируем категориальыне фичи
            if len(self.categorical_features) > 0:
                data[self.categorical_features]  = self.lab_encoder.transform(data[self.categorical_features]) # применяем
                self.model_embends = True
                print('Былы примененя категоризация переменных.')
                
        data = data.astype(self.features) # перобразуем фичи
        
        # семплируем
        data['SN']       = data.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True])\
                                .groupby(['CUSTOMER_ID']).cumcount() + 1
        
        customers        = data.groupby(['CUSTOMER_ID'])['SN'].max().reset_index()
        customers        = customers[customers['SN'].between(*self.seq_nums)]['CUSTOMER_ID'].unique()        
        data             = data[data['CUSTOMER_ID'].isin(customers)]    
    
        # нормализуем фичи
        if hasattr(self, 'scaler') and isinstance(self.scaler, MinMaxScaler):
            data[self.features_list]      = self.scaler.transform(data[self.features_list])

        return data

    def create_tensor(self, data):
        # строим тензор
        start_time       = time.time()
        cube             = tensor.Parallels3DCube()
        target_col       = self.type_of_model
        data[target_col] = 0
        sequence_length  = self.metadata['sequence_length']#10 # подтягиваем из метаданных модели
        numeric_features = [feature for feature in self.features_list if feature not in self.categorical_features]
        
        print("Считаются числовые фичи")
        X, y, df = cube.parallelize_pairs(self.store_clients_pairs, 
                                          data, 
                                          n_cores = self.n_cores, 
                                          sequence_length=sequence_length, 
                                          features = numeric_features, 
                                          target = target_col, 
                                          groupb_columns=['CUSTOMER_ID']) 
        
        if self.model_embends == True:
            print("Считаются категориальные фичи")
            X_cat, _, df_cat = cube.parallelize_pairs(self.store_clients_pairs, 
                                                      data, 
                                                      n_cores = self.n_cores, 
                                                      sequence_length=sequence_length, 
                                                      features = self.categorical_features, 
                                                      target = target_col , 
                                                      groupb_columns=['CUSTOMER_ID'])
            

        self.model_data['data'] = df.to_dict(orient='list')
        
        print("--- %s parallels ---" % (time.time() - start_time))

        return (X, X_cat) if self.model_embends else (X, None)
    def normalize_X (self, X):
        # если нужно выполнить скейлинг
        if hasattr(self, 'X_scaler') and isinstance(self.X_scaler, MinMaxScaler) and hasattr(self, 'y_scaler') and isinstance(self.y_scaler, MinMaxScaler):
            print('Нормализация X')
            # Преобразование 3D массивов в 2D массивы
            shape_data            = X.shape
            data_array_2d         = X.reshape(-1, shape_data[-1])
            data_array_2d_scaled  = self.X_scaler.transform(data_array_2d)

            # Восстановление исходной формы 3D массивов
            X = data_array_2d_scaled.reshape(shape_data)
            
            self.inverse_y = True
        return X
        
    def prediction(self, X, X_cat):
        start_time = time.time()
        if self.model_embends == True:
            X     = np.asarray(X).astype(np.float32)
            X_cat = X_cat.astype(np.int32)

            '''
            чтобы избежать ошибки  InvalidArgumentError: Graph execution error 
            вызвана тем, что индекс, подаваемый на слой Embedding, находится за пределами диапазона.
            '''
            max_category_values = self.metadata.get('max_category_values', None) 
            if max_category_values != None:
                for i in range(len(self.categorical_features)):
                    # Определяем максимальное значение категории из обучающей выборки
                    max_value = max_category_values[i]

                    # "Резервный" индекс - это max_value + 1
                    reserve_index = max_value

                    # Для каждой фичи заменяем категории, которые превышают max_value, на reserve_index
                    X_cat[:, :, i] = np.where(X_cat[:, :, i] > max_value, reserve_index, X_cat[:, :, i])

                X_cat2 = [X_cat[:, :, i] for i in range(4)]

                prediction  = self.model.predict(X_cat2 + [X])
                
        else:
            X           = np.asarray(X).astype(np.float32)
            prediction  = self.model.predict(X) 
            
        end_time       = time.time()
        execution_time = end_time - start_time  # Подсчет времени выполнения
        print(f"Время выполнения: {execution_time:.4f} секунд")
        
        # если таргет масштабировался
        if self.inverse_y:
            prediction = self.y_scaler.inverse_transform(prediction)
        
        self.model_data['prediction']       = prediction
        self.model_data['optimal_treshold'] = self.metadata.get('treshold', None) # если есть значение , передаем, иначе None
    
    def main(self, df_base):
        data = df_base.copy()
        # составляем список клиентов
        self.store_clients_pairs = list(set(df_base['CUSTOMER_ID']))
        if self.load_data() == 'Error':
            return
        data     = self.preproc(data)
        X, X_cat = self.create_tensor(data)
        X        = self.normalize_X(X)
        
        print('Размер датасета в Mb: ', functions.numpy_array_memory_usage_mb(X))
        
        self.prediction(X, X_cat)
        print('Модель отработала успешно')
        print()
        return self.model_data