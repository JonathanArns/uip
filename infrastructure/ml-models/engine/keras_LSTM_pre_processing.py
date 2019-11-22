"""
basic imports for script
"""
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

import plotly as py
import plotly.offline as pyoff
import plotly.graph_objs as go
"""
CONSTANTS
"""

CONFIG_PATH = os.path.dirname(os.path.abspath(__file__)) + '/.config/processing_config.json'
print('CONF')
print(CONFIG_PATH)
#######HELPER FUNCTIONS##############################################################

def __init_logging():
    """
    setting up the logger for error tracking
    """
    fileDir = os.path.dirname(os.path.abspath(__file__))
    path_to_log = os.path.dirname(fileDir) + '/logs/processing-logs.log'
    print(path_to_log)
    logging.basicConfig(filename=path_to_log, 
                        filemode='a', 
                        format='%(asctime)s:%(levelname)s-%(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)


def _load_config_json():
    """
    loading the config file in order to get target, feature, date, and groupby value
    assigns the constant CONFIG the json as dict{}
    """
    try:
        with open(CONFIG_PATH, 'r') as conf:
            config = json.load(conf)
            return config
    except Exception as json_error:
        print(cp(f'log[error]:unable to load preprocessing.config.json file. See log file', 'red'))
        logging.error('unable to load preprocessing.config.json file', exc_info=True)


def _get_key_value_config(key, config):
    """
    function returning the value of passed key
    param:: key:string
    return:: value:string
        """
    try:
        return config['lstm-script'][key]
    except KeyError as key_error:
        print(cp(f'log[error]:Key not in config dictornay. See log file', 'red'))
        logging.error('Key not in config dictorna', exc_info=True)

def save_data_to_(file_name, folder_name , data, is_model):
    """
    function to save model and scaler.
    Needed to change the current directory 
    The is_model defindes which saver to use. for scaler and model two different needed
    param:: file_name:string, folder_name:string, data:obj, is_model:boolean
    return:: -
    raise: Execption
    """
    try:
        os.chdir('infrastructure/ml-models/'.join(folder_name))
        if is_model:
            data.save(file_name)
        else:
            joblib.dump(data, file_name)
    except Exception as saving_error:
        print(cp(f'log[error]:Failed to save data for {file_name}. See log file', 'red'))
        logging.error('Failed to save data for', exc_info=True)
    finally:
        os.path.dirname(os.path.abspath(__file__))


######################################################################################################################
def _compute_diff_of_sales(sales, prev):
    """
    function to compute the difference between the prev_sales and the sales
    prev_sales beeing the sales shited at index 1
    param:: sales:pandas.DataFrame, prev:pandas.DataFrame
    return diff:pandas.DataFrame
    """
    return (sales - prev)


def _create_lags_for_supervised(df, n):
    """
    function which will create new columns (lag_1 - lag_1+n)
    df with lags 1 - n with shifted sales will be returned
    param:: df:pandas.DataFrame, n:int
    return:: df:pandas.DataFrame
    """
    for inc in range(1,n):
        field_name = 'lag_' + str(inc)
        df[field_name] = df['difference'].shift(inc)
    df = df.dropna().reset_index(drop=True)
    return df



def _split_to_train_test(df):
    """
    function spliting the data frame into train and test set
    param:: df:pandas.DataFrame
    return:: train:numpy.array, test:numpy.array
    """
    return df[0:-6].values, df[-6:].values 


def _min_max_scaling(train, test):
    """
    using MinMax function for scaling array in range(-1,1)
    saving scaler in order to reuse it for precdictions
    param:: train:numpy.array, test:numpy.array
    return:: train_scaled:numpy.array, test_scaled:numpy.array
    s"""
    scaler = MinMaxScaler(feature_range=(-1,1))
    scaler = scaler.fit(train)

    # reshaping the array so they fit in the scaler
    train = train.reshape(train.shape[0],train.shape[1])
    test  = test.reshape(test.shape[0], test.shape[1])


    train_scaled = scaler.transform(train)
    print(f'Scaler: {test.shape}')
    test_scaled  = scaler.transform(test)

    # svaing the scaler
    try:
        save_data_to_(
        file_name='min-max-scaler.save',
        folder_name='saved_model_data',
        data=scaler,
        is_model=False)
    except Exception as save_error:
        print(cp(f'log[error]:Failed to save data for \'min-max-scaler.save\'. See log file', 'red'))
        logging.error('Failed to save data for', exc_info=True)
    
    return train_scaled, test_scaled



def _create_LSTM_model(X,y):
    """
    creating the LSTM Sequential Model. (Parameters X,y are X_train, y_train)
    using 4 output nodes
    1 Hidden-Layer (Dense)
    loss-function= mean squared erorr
    param:: X:numpy.array, y:numpy.array
    """
    model = Sequential()
    model.add(
        LSTM(4, batch_input_shape=(1, X.shape[1], X.shape[2]), stateful=True)
    )
    model.add(Dense(1))
    model.compile(
        loss='mean_squared_error',
        optimizer='adam'
    )
    model.fit(
        X, y, 
        nb_epoch=120,
        batch_size=1,
        verbose=1,
        shuffle=False
    )
    return model




#####################################################################################


def start_processor():
    """
    this is the main function with orchastrats the programm
    first the data (csv) will be load - as well as the config file. The data frame will be reduced to the data needed.
    next the differences of sales and prev-sales are computed followed by the creation of lags needed for the LSTM model
    as last step the LSTM model will be saved (both architecture and weights) in a .h5 file for later usage.
    In addition the fitted MinMaxScaler will be saved so it can be imported with the model.h5 for making proper predictions
    param:: -
    return:: -
    """
    __init_logging()

    # fetch config file, extract values
    config = _load_config_json()
    df_path    = _get_key_value_config('dataFrame-path', config)
    print(df_path)
    date_col   = _get_key_value_config('date-col', config)
    target_col = _get_key_value_config('target-col', config) 

    #loading data frame
    data = pd.read_csv(df_path, encoding='ISO-8859-1', sep=",")
    data = data.rename(columns={date_col: 'date', target_col: 'sales'})
    date_col = 'date'
    target_col = 'sales'
    # data frame with Sales and Date only
    print(data.head())
    df_lstm = data.loc[:,(date_col, target_col)].copy()
    
    # convert date field to pandas.Datetime obj and group by month
    df_lstm[date_col] = pd.to_datetime(df_lstm[date_col])
    df_lstm[date_col] = df_lstm[date_col].dt.year.astype('str') + '-' + df_lstm[date_col].dt.month.astype('str') + '-01'
    df_lstm[date_col] = pd.to_datetime(df_lstm[date_col])

    df_lstm = df_lstm.groupby(date_col).sales.sum().reset_index()
    #############################################################

    # comupting the difference between sales and prev sales. Adding the lags to the data frame
    df_diff = df_lstm.copy()
    df_diff['prev_sales'] = df_diff['sales'].shift(1)
    print(f'diff  {df_diff}')
    df_diff= df_diff.dropna()
    print(f'diff drop {df_diff}')
    df_diff['difference'] = _compute_diff_of_sales(df_diff['sales'], df_diff['prev_sales'])
    print(f'diff shape {df_diff.shape}')
    df_supervised = df_diff.drop(['prev_sales'], axis=1).copy()
    df_supervised = _create_lags_for_supervised(df_supervised, 12)

    df_features = df_supervised.drop(['sales', 'date'], axis=1)
    #############################################################

    # split data frame in train test sets and scale both arrays
    train_set, test_set = df_features[0:-6].values, df_features[-6:].values
    train_scaled, test_scaled = _min_max_scaling(train_set, test_set)

    #############################################################
   
    # creating feature and target variables. Model creation / fiting and saving
    X_train, y_train = train_scaled[:, 1:], train_scaled[:, 0:1]
    X_train = X_train.reshape(X_train.shape[0], 1, X_train.shape[1]) 

    X_test, y_test  = test_scaled[:, 1:], test_scaled[:, 0:1]

    X_test  = X_test.reshape(X_test.shape[0], 1, X_test.shape[1])

    lstm_model = _create_LSTM_model(X_train, y_train)

    try:
        save_data_to_(
        file_name='lstm-model.h5',
        folder_name='saved_model_data',
        data=lstm_model,
        is_model=True
        )
    except Exception as save_error:
        print(cp(f'log[error]:Failed to save data for \'lstm-model.h5\'. See log file', 'red'))
        logging.error('Failed to save data for', exc_info=True)

    
    
if __name__ == '__main__':
    start_processor()