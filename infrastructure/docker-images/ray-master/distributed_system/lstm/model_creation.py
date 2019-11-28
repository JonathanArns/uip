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
    """
    LSTM class if instanciated will be able to make predictions based on the model loaded.
    From the instance one only has to call the predict(data) function.
    The class will then take care of any scaling shaping and inverse scaling
    """
    def __init__(self):
        self.model  = self._load_model()
        self.scaler = self._load_scaler()
        self.start = None


    def _load_model(self):
        """
        function importing the LSTM model from an .h5 file
        param:: -
        return:: lstm_model:keras.LSTM
        raise:: KerasError
        """
        try:
            return load_model('/usr/src/app/distributed_system/lstm/model_data/lstm-model.h5')
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
            return joblib.load('/usr/src/app/distributed_system/lstm/model_data/min-max-scaler.save')
        except Exception as joblib_loader_error:
            raise joblib_loader_error

    def predict(self, data):
        """
        function gets a json string with the data
        Json will be decoded, scaled and the run through the prediction.
        The predictions will be stacked in a dictonary pleasing the needs kafka-connect wants a schema for writing in the db
        param:: data:json
        return:: inversed_pred: dictonary{}
        """
        try:
            y_data, self.start = self._decode_json_to_df(data)
            y_scaled           = self._feature_scaling(self.scaler, y_data)
            y_pred             = self.model.predict(y_scaled, batch_size=1)
            print(y_pred.shape)
            y_pred             = self._inverse_scaling(y_pred)
            back = self._encode_data_to_json(self.start, y_pred)

            return back
        except Exception as prediction_error:
            raise prediction_error



##############TOOL-BOX-FUNCTIONS##############################################

    def _decode_json_to_df(self, data):
        """
        in order to transform the json to a pandas Data Frame we need to load the json in a dictonary.
        param:: data:json
        return:: pandas.DataFrame, fist_data:string
        raise:: jsonLoadError, pandasDataFrameError
        """
        try:
            data_ = json.loads(data)
            return pd.DataFrame(data_['data']), data_['data'][0]['date']
        except Exception as parser_error:
            raise parser_error


    def _encode_data_to_json(self, start, data):
        """
        this dictonary structrue is required for kafka and kafka-connect in order to save the results in the db
        param:: start:string, data:np.array
        return:: payload:list[dict{}]
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
                                'type': "float64",
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



    def _lag_creation(self, df):
        """

        """
        for inc in range(1,11):
            field_name = 'lag_' + str(inc)
            df[field_name] = df['difference'].shift(inc)
        df = df.dropna().reset_index(drop=True)
        print(f'shape after lags: {df.shape}')
        return df


    def _feature_scaling(self, scaler, data):
        """
        TODO: docs
        """
        data['sales']      = data['sales'].astype(float)
        data['prev_sales'] = data['sales'].shift(1)
        data               = data.dropna()
        data['difference'] = (data['sales'] - data['prev_sales'])
        data               = data.drop(['prev_sales'], axis=1).copy()
        lag_df             = self._lag_creation(data)

        lag_df             = lag_df.drop(['sales', 'date'], axis=1)
        lag_df             = lag_df.values

        lag_df             = lag_df.reshape(lag_df.shape[0], lag_df.shape[1])
        lag_df             = np.concatenate([lag_df, [[0],[0],[0],[0],[0],[0]]], axis=1)
        print(lag_df.shape)
        y                  = scaler.transform(lag_df)
        y                  = np.delete(y, -1, axis=1)
        y                  = y.reshape(y.shape[0], 1, y.shape[1])

        return y


    def _inverse_scaling(self, y_pred):
        """
        TODO: docs
        """
        tmp = []
        for i in range(0,len(y_pred)):
            tmp.append(
                np.concatenate([[[0,0,0,0,0,0,0,0,0,0,0]], [y_pred[i]]], axis=1)
            )
        tmp = np.array(tmp)
        tmp = tmp.reshape(tmp.shape[0], tmp.shape[2])
        tmp = self.scaler.inverse_transform(tmp)
        return tmp[:,-1]
