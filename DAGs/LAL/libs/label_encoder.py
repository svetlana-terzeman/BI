import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import mlflow
import mlflow.pyfunc

class MultiFeatureLabelEncoder(mlflow.pyfunc.PythonModel):
    def __init__(self, features=None):
        """
        Инициализация класса.

        Parameters:
        features (list of str): Список фичей, для которых будет применяться LabelEncoder.
        """
        self.features = features
        self.label_encoders = {feature: LabelEncoder() for feature in features}
        self.classes_ = {feature: set() for feature in features}

    def fit(self, X):
        """
        Обучение LabelEncoder для каждой фичи.

        Parameters:
        X (DataFrame): Датафрейм, содержащий обучающую выборку.
        """
        for feature in self.label_encoders:
            self.label_encoders[feature].fit(X[feature])
            self.classes_[feature] = set(self.label_encoders[feature].classes_)
        return self

    def transform(self, X):
        """
        Преобразование данных для каждой фичи с учетом новых классов.

        Parameters:
        X (DataFrame): Датафрейм, содержащий данные для преобразования.

        Returns:
        DataFrame: Датафрейм с закодированными фичами.
        """
        X_transformed = X.copy()
        for feature in self.label_encoders:
            y = np.array(X[feature])
            new_classes = set(y) - self.classes_[feature]

            if new_classes:
                self.label_encoders[feature].classes_ = np.append(self.label_encoders[feature].classes_, list(new_classes))
                self.classes_[feature] = set(self.label_encoders[feature].classes_)

            X_transformed[feature] = self.label_encoders[feature].transform(X[feature])
        return X_transformed