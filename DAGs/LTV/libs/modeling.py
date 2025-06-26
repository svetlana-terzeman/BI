import pandas as pd
from catboost import cv, Pool, CatBoostClassifier
from libs.label_encoder      import MultiFeatureLabelEncoder
from sklearn.utils.class_weight import compute_class_weight
from sklearn.metrics import roc_auc_score
from catboost.utils import eval_metric

import matplotlib.pyplot as pyplot

from IPython.display import display
from sklearn.calibration import calibration_curve, CalibratedClassifierCV
from sklearn.model_selection import train_test_split, GridSearchCV
from xgboost import XGBClassifier


class Catboost_classificator:

    @staticmethod
    def catboost_base_model_func(
                                     X_train,
                                     y_train,
                                     X_val,
                                     y_val,
                                     cat_feature,
                                     params={}
                                ):
        # создаем объект класса катбуст с дефолтными параметрами
        base_model = CatBoostClassifier(eval_metric='AUC',
                                        auto_class_weights='Balanced',
                                        early_stopping_rounds=10,
                                        random_state=0,
                                        use_best_model = True,
                                        cat_features=cat_feature,
                                        **params
                                       )

        # обучаем катбуст
        base_model.fit(X_train, y_train, eval_set=(X_val, y_val), verbose=True)
        return base_model


  