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
    The class will then take care of any scaling, shaping and inverse scaling.
    """
    def __init__(self):
        self.model  = self._load_model()
        self.scaler = self._load_scaler()
        self.start = None
        self.last_sale = None

    def _load_model(self):
        """
        Imports LSTM model from an .h5 file.\n
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
        Imports min / max scaler from an .save file.\n
        param:: -
        return:: sklearn.preprocessing.MinMacScaler
        raise:: sklearn.joblib.error
        """
        try:
            return joblib.load('/usr/src/app/distributed_system/lstm/model_data/min-max-scaler.save')
        except Exception as joblib_loader_error:
            raise joblib_loader_error

    def predict(self, data):
        """
        Data will be decoded, scaled, shaped and the run through the prediction.
        Predictions will be returned in multiple dictionaries, containing schema and payload to conform to Kafka Connect standarts.
        Takes 17 months of sales figures and return 6 months of predicted sales figures.\n
        param:: data: string (json)
        return:: list[dict{}]
        """
        try:
            y_data, self.start, self.last_sale = self._decode_json_to_df(data)
            y_scaled           = self._feature_scaling(self.scaler, y_data)
            y_pred             = self.model.predict(y_scaled, batch_size=1)
            print(y_pred.shape)
            y_pred             = self._inverse_scaling(y_pred)
            back = self._encode_data_to_json(self.start, y_pred, self.last_sale)

            return back
        except Exception as prediction_error:
            raise prediction_error



##############TOOL-BOX-FUNCTIONS##############################################

    def _decode_json_to_df(self, data):
        """
        Parses a json string and returns it as a pandas.DataFrame.\n
        param:: string (json)
        return:: pandas.DataFrame, string (latest date), string (latest sales)
        raise:: jsonLoadError, pandasDataFrameError
        """
        try:
            data_ = json.loads(data)
            return pd.DataFrame(data_['data']), data_['data'][len(data_['data'])-1]['date'], data_['data'][-6:]
        except Exception as parser_error:
            raise parser_error


    def _encode_data_to_json(self, start, data, prev_data):
        """
        Aggregates data and prev_data to get actual predicted sales figures and formats them into dictionaries to represent json.\n
        param:: start:string, data:numpy.array, prev_data:numpy.array
        return:: list[dict{}]
        """

        date_list = start.split('-')

        next_six_month = []
        for i in range(0,6):
            if date_list[1] == '12':
                year = int(date_list[0]) + 1
                date_list[0] = str(year)
                month = '01'
                date_list[1]  = month
            else:
                month = str(int(date_list[1]) + 1)
                if len(month) < 2:
                    month = '0' + month
                date_list[1]  = month

            string_date = str(date_list[0])+'-'+str(date_list[1])+'-'+str(date_list[2])
            next_six_month.append(string_date)

        act_values = self._inverse_lag(prev_data)

        payload = []
        for date, field in zip(next_six_month, act_values):
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
                                'field': "sales"
                            }],
                    'optional':False,
                    'name':'com.github.jcustenborder.kafka.connect.model.Value'
                },
                'payload': {
                    'date': str(date),
                    'sales': str(field)
                }
            }
            payload.append(d)
        return payload


    def _inverse_lag(self, sales):
        """
        Reverses lags and differencing on the model output.\n
        param:: sales:pandas.DataFrame
        return:: list[float]
        """
        inverse_sales = []
        for sale, prev in zip(sales, self.last_sale):
            inverse_sales.append(prev['sales'] + sale['sales'])
        return inverse_sales

    def _lag_creation(self, df):
        """
        Reshapes a differenced timeseries DataFrame into two dimensions (6,11)
        and creates 6 smaller timeseries that are shifted by one month each,
        so that the result represents 16 differenced months or 17 months overall.\n
        param:: df:pandas.DataFrame
        return:: pandas.DataFrame
        """
        for inc in range(1,11):
            field_name = 'lag_' + str(inc)
            df[field_name] = df['difference'].shift(inc)
        df = df.dropna().reset_index(drop=True)
        return df


    def _feature_scaling(self, scaler, data):
        """
        Scales model input using Min Max algorithm.\n
        param:: scaler:sklearn.preprocessing.MinMaxScaler, data:pandas.DataFrame
        return:: numpy.array
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
        Scales model output in reverse.\n
        param:: y_pred:numpy.array
        return:: numpy.array
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
