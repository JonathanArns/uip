import pandas as pd
import numpy as np
import logging
import json
import os
from termcolor import colored as cp

"""
imports for machine learning model and preprocessing
"""
import keras
from keras.layers import LSTM, Dense
from keras.models import Sequential, load_model
from keras.optimizers import Adam
from keras.callbacks import EarlyStopping
from keras.utils import np_utils
from sklearn.preprocessing import MinMaxScaler
from sklearn.externals import joblib



class LSTM():
    def __init__(self):
        self.model  = self._load_model()
        self.scaler = self.load_scaler()
        self.start = None
    

    def _load_model(self):
        """
        function importing the LSTM model from an .h5 file
        param:: -
        return:: lstm_model:keras.LSTM
        raise:: KerasError
        """
        try:
            return load_model('infrastructure/docker-images/ray-master/distributed_system/lstm/model_data/lstm-model.h5')
        except Exception as keras_loader_error:
            raise keras_loader_error

    def _load_scaler(self):
        """
        function importing the min / max from an .save file
        param:: -
        return:: scaler:sklearn.preprocessing.MinMacScaler
        raise:: sklearn.joblib.error
        """
        try:
            return joblib.load('infrastructure/docker-images/ray-master/distributed_system/lstm/model_data/min-max-scaler.save')
        except Exception as joblib_loader_error:
            raise joblib_loader_error

    def predict(self, data):
        """
        TODO: docs
        """
        y_data = _feature_scaling(data)
        try:
            y_data, self.start = _decode_json_to_df(data)
            y_scaled           = _feature_scaling(self.scaler, data)
            y_pred             = self.model.predict(y_scaled, batch_size=1)
            return _encode_data_to_json(self.start, y_pred.flatten())
        except Exception as prediction_error:
            raise prediction_error

    

##############TOOL-BOX-FUNCTIONS##############################################
    @staticmethod
    def _decode_json_to_df(data):
        """
        in order to transform the json to a pandas Data Frame we need to load the json in a dictonary.
        param:: data:json
        return:: pandas.DataFrame, fist_data:string
        raise:: jsonLoadError, pandasDataFrameError
        """
        try:
            data_ = json.load(data)
            return pd.DataFrame(data_['data']), data_['data'][0]['date']
        except Exception as parser_error:
            raise parser_error
    

    @staticmethod
    def _encode_data_to_json(start, data):
        """
        TODO: use date insted of ID
        """
        payload = []
        for i, field in enumerate(data):
            d = {
                'schema': {
                    'type': 'struct',
                    'fields': [
                            {
                                'type': "string",
                                'optional': False,
                                'field': "date"
                            },
                            {
                                'type': "string",
                                'optional': False,
                                'field': "date"
                            }],
                    'payload': {
                        'date': str(i),
                        'sales': field
                        }
                    }
                }
            payload.append(d)
        return payload



    @staticmethod
    def _lag_creation(df):
        """
        TODO: docs
        """
        for inc in range(1,13):
            field_name = 'lag_' + str(inc)
            df[field_name] = df['difference'].shift(inc)
            df = df.dropna().reset_index(drop=True)
        return df

    @staticmethod
    def _feature_scaling(scaler, data):
        """
        TODO: docs
        """
        data['prev_sales'] = data['sales'].shitf(1)
        data               = data.dropna()
        data['difference'] = (data['sales'] - data['prev_sales'])

        lag_df             = _lag_creation(data)
        lag_df             = lag_df.drop(['sales', 'date'], axis=1)
        lag_df             = lag_df.reshape(lag_df[0], lag_df.shape[1])

        y                  = scaler.transform(lag_df.values)
        y                  = y.reshape(y.shape[0], 1, y.shape[1])
        return y

    @staticmethod
    def inverse_scaling(scaler, y_pred):
        """
        TODO: docs
              create seperat scaler ONLY for predicted values!
        """
        pass