import pandas as pd
import time
import datetime
import numpy as np
from libs.label_encoder      import MultiFeatureLabelEncoder

class FeatureEngineering():
    
    def __init__(self):
        pass
    
    # Функция для нахождения самой частой категории с учетом равенства частот
    def most_frequent_category(self,categories):
        modes          = categories.mode()
        value_counts   = categories.value_counts()
        unique_values  = categories.nunique()
    
        # Если только одно уникальное значение, возвращаем его
        if unique_values == 1:
            return categories.iloc[0]
                
        #print(value_counts)
        if (value_counts == 1).all():
            return np.nan
        if len(modes) > 1:
            return np.random.choice(modes) # "_".join(modes)#
        return modes[0]
    
    # основная функция создания базовых фичей
    def GeneratedBase(self, 
                      df,
                      customer_ids = [], 
                      curdate = pd.to_datetime(datetime.datetime.today()).date(), # по умолчанию текущая дата расчета
                      items=[
                                'CUSTOMER_ID', 
                                'CASSTICKID',
                                'FIRSTORDERDATE',
                                'TRADE_DT',
                                'REGION_NAME_EN',
                                'USER_TYPE',
                               # 'USER_LEVEL',
                                'IDENTIFICATION',
                                'IDENTIFICATION_INDEX',
                            #    'OBJECT_FORMAT_BK'
                            ],
                      
                      ltv_research_days = 366
                      
                     ):
    
        # если нужно посчитать по какому то одному примеру
        if len(customer_ids) > 0:
            features = df[df['CUSTOMER_ID'].isin(customer_ids)].copy()

        else:
            features = df.copy()
        
        print(items)
        
        features = features.groupby(items).agg({
                                                                                          'PRICE':           ['min','max','mean','sum'], 
                                                                                          'PRODUCT_CODE':    ['nunique','count'], 

                                                                                           'MART_NAME_RU':    self.most_frequent_category,  
                                                                                           'SEGMENT_NAME_RU': self.most_frequent_category, 
                                                                                           'CATEGORY_NAME_RU':self.most_frequent_category, 
                                                                                           'FAMILY_NAME_RU':  self.most_frequent_category

                                                                                     }).sort_values(by=['CUSTOMER_ID','TRADE_DT', 'IDENTIFICATION_INDEX'], ascending=[True,True, True]).reset_index()



        features.columns                           = ["".join(s) for s in features.columns.ravel()]
        features                                   = features.sort_values(['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True]) # после группировки и сброса восстанавлвиаем сортировку
        
        # бывают случае что в разных заказах меняется пол. чтобы не исключать данное поле решил пока что брать саммое популярное значение
        features['USER_TYPE']                     = features.groupby(['CUSTOMER_ID'])['USER_TYPE'].transform(lambda x: x.mode().iloc[0] if not x.mode().empty else None)


        features['previous_date']                  = features.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True]).groupby(['CUSTOMER_ID'])['TRADE_DT'].shift()
        features['order_diff']                     = ((features['TRADE_DT'] - features['previous_date']) / np.timedelta64(1, 'D')).fillna(0)
        features['order_diff_cum']                 = (features['TRADE_DT'] - features['FIRSTORDERDATE']) / np.timedelta64(1, 'D')



        features                                   = features[features['order_diff_cum'] <= ltv_research_days]
        features['LTV']                            = features.groupby('CUSTOMER_ID')['PRICEsum'].transform('sum')
        features['LTV_ONLINE']                     = features[features['IDENTIFICATION'] == 'ONLINE'].groupby('CUSTOMER_ID')['PRICEsum'].transform('sum')
        features['LTV_ONLINE']                     = features.groupby('CUSTOMER_ID')['LTV_ONLINE'].apply(lambda x: x.ffill().bfill())


        features = features.reset_index() # произошел фильтр заказов, переобпределяем порядок индексов

        # features['previous_region']                = features.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True]).groupby(['CUSTOMER_ID'])['REGION_NAME_EN'].shift()
        # features['region_changing']                = features.apply(lambda a: 0 if a['previous_region'] == a['REGION_NAME_EN'] or a['previous_region'] is np.nan else 1, axis = 1)
        
        
        # 1. Получаем предыдущий регион с учетом нужной сортировки
        features                                   = features.sort_values(by=['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True, True, True, False, True])
        features['previous_region']                = features.groupby('CUSTOMER_ID')['REGION_NAME_EN'].shift()
        # 2. Флаг смены региона
        #features['region_changing']                = features.apply( lambda a: 0 if pd.isna(a['previous_region']) or a['previous_region'] == a['REGION_NAME_EN'] else 1, axis=1)
        features['region_changing']                = ((features['previous_region'].notna()) & (features['previous_region'] != features['REGION_NAME_EN'])).astype(int)


        features['PRICEcumsum']                    = features.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True]).groupby(['CUSTOMER_ID'])['PRICEsum'].cumsum()

        features['PRICEexpanding_sum_mean']        =  features.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True]).groupby('CUSTOMER_ID')['PRICEsum'].expanding().mean().reset_index(drop=True)

        features['PRICEexpanding_mean_mean']       =  features.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True]).groupby('CUSTOMER_ID')['PRICEmean'].expanding().mean().reset_index(drop=True)

        features['PRICEpct_change']                = (features.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True]).groupby(['CUSTOMER_ID'])['PRICEsum'].pct_change() * 100).fillna(0).round(3)

        # features                                   = features.merge(features[features['TRADE_DT'] == features['FIRSTORDERDATE']][[ 'CUSTOMER_ID', 'PRICEsum']].rename({'PRICEsum': 'first_order_price'}, axis = 1), \
        #                                                            on = 'CUSTOMER_ID',how = 'left')
        features['first_order_price']              = features.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True]).groupby('CUSTOMER_ID')['PRICEsum'].transform('first') 
        features['sequence_number']                = features.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True]).groupby('CUSTOMER_ID').cumcount() + 1
        features['lifetime_day']                   = ((curdate - pd.to_datetime(features['FIRSTORDERDATE']).dt.date) / np.timedelta64(1, 'D')).fillna(0)
        features['pct_change_base']                = (((features['PRICEsum'] - features['first_order_price']) / features['first_order_price']) * 100).fillna(0).round(3)
        features['LTV_ONLINE_PRT']                 = np.round(features['LTV_ONLINE'] / features['LTV']  * 100,1)
        
        features                                   = features.sort_values(by = ['CUSTOMER_ID', 'TRADE_DT', 'IDENTIFICATION_INDEX', 'PRICEsum', 'CASSTICKID'], ascending=[True,True, True, False, True])
        
        return features