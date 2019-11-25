"""
basic imports for script
"""
import pandas as pd
import numpy as np
import logging
import json
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
CONFIG_PATH = '/Users/konstantingasser/Desktop/ml-models/engine/.config/processing.config.json'
#######HELPER FUNCTIONS##############################################################
"""
setting up the logger for error tracking
"""
def __init_logging():
    logging.basicConfig(filename='processing-logs.log', 
                        filemode='a', 
                        format='%(asctime)s:%(levelname)s-%(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

"""
loading the config file in order to get target, feature, date, and groupby value
assigns the constant CONFIG the json as dict{}
"""
def _load_config_json():
    try:
        with open(CONFIG_PATH, 'r') as conf:
            config = json.load(conf)
            return config
    except Exception as json_error:
        print(cp(f'log[error]:unable to load preprocessing.config.json file. See log file', 'red'))
        logging.error('unable to load preprocessing.config.json file', exc_info=True)

"""
function returning the value of passed key
param:: key:string
return:: value:string
"""
def _get_key_value_config(key, config):
    try:
        return config['lstm-script'][key]
    except KeyError as key_error:
        print(cp(f'log[error]:Key not in config dictornay. See log file', 'red'))
        logging.error('Key not in config dictorna', exc_info=True)

"""
function to compute the difference between the prev_sales and the sales
prev_sales beeing the sales shited at index 1
param:: sales:pandas.DataFrame, prev:pandas.DataFrame
return diff:pandas.DataFrame
"""
def _compute_diff_of_sales(sales, prev):
    return (sales - prev)


"""
function which will create new columns (lag_1 - lag_1+n)
df with lags 1 - n with shifted sales will be returned
param:: df:pandas.DataFrame, n:int
return:: df:pandas.DataFrame
"""
def _create_lags_for_supervised(df, n):
    for inc in range(1,n):
        field_name = 'lag_' + str(inc)
        df[field_name] = df['difference'].shift(inc)
    df = df.dropna().reset_index(drop=True)
    return df


"""
function spliting the data frame into train and test set
param:: df:pandas.DataFrame
return:: train:numpy.array, test:numpy.array
"""
def _split_to_train_test(df):
    return df[0:-6].values, df[-6:].values 

"""
using MinMax function for scaling array in range(-1,1)
saving scaler in order to reuse it for precdictions
param:: train:numpy.array, test:numpy.array
return:: train_scaled:numpy.array, test_scaled:numpy.array
"""
def _min_max_scaling(train, test):
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
        scaler_file = 'scaler_lstm.save'
        joblib.dump(scaler, scaler_file)
    except Exception as joblib_scaler_save_error:
        print(cp(f'log[error]:joblib failed to save the scaler. See log file', 'red'))
        logging.error('joblib failed to save the scaler', exc_info=True)
    
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
    __init_logging()

    # fetch config file, extract values
    config = _load_config_json()
    df_path    = _get_key_value_config('dataFrame-path', config)
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
    y_pred = lstm_model.predict(X_test, batch_size=1)
    # print('The real shit')
    # print(scaler.inverse_transform(y_pred))
    lstm_model.save('lstm_flink.h5')

    # scaler = MinMaxScaler(feature_range=(-1,1))
    # scaler = scaler.fit(scaler_data)
    # joblib.dump(scaler, 'feature_scaler.save')
    # del scaler
    # scaler = joblib.load('/Users/konstantingasser/workspaces/vs_code-workspace/uip_sap/project_sourceCode/feature_scaler.save')
    # scaler.transform(scaler_data)
    
    # Yscaler = MinMaxScaler(feature_range=(-1,1))
    # Yscaler.fit(ytest)
    # joblib.dump(Yscaler, 'Y_inverse_scaler.save')
    # print(ytest.shape)
    # print(y_pred)
    
    y_pred = y_pred.reshape(y_pred.shape[0], 1, y_pred.shape[1])
    #rebuild test set for inverse transform
    pred_test_set = []
    for index in range(0,len(y_pred)):
        # print (np.concatenate([y_pred[index],X_test[index]],axis=1))
        pred_test_set.append(np.concatenate([y_pred[index],X_test[index]],axis=1))
    #reshape pred_test_set
    pred_test_set = np.array(pred_test_set)
    pred_test_set = pred_test_set.reshape(pred_test_set.shape[0], pred_test_set.shape[2])
    #inverse transform
    scaler = MinMaxScaler(feature_range=(-1,1))
    scaler = scaler.fit(train_set)
    pred_test_set_inverted = scaler.inverse_transform(pred_test_set)
    print('pred')
    print(pred_test_set_inverted)
    print(pred_test_set_inverted[:,0])
    #create dataframe that shows the predicted sales
    result_list = []    
    sales_dates = list(df_lstm[-7:].date)
    act_sales = list(df_lstm[-7:].sales)
    for index in range(0,len(pred_test_set_inverted)):
        result_dict = {}
        result_dict['pred_value'] = int(pred_test_set_inverted[index][0] + act_sales[index])
        result_dict['date'] = sales_dates[index+1]
        result_list.append(result_dict)
    df_result = pd.DataFrame(result_list)

    #merge with actual sales dataframe
    df_sales_pred = pd.merge(df_lstm,df_result,on='date',how='left')
    #plot actual and predicted
    plot_data = [
        go.Scatter(
            x=df_sales_pred['date'],
            y=df_sales_pred['sales'],
            name='actual'
     ),
        go.Scatter(
                x=df_sales_pred['date'],
                y=df_sales_pred['pred_value'],
                name='predicted'
    )
    
    ]
    plot_layout = go.Layout(
        title='Sales Prediction'
    )
    fig = go.Figure(data=plot_data, layout=plot_layout)
    pyoff.iplot(fig)
    y_pred = np.array([
                        [4238.7042747735195],
                        [-20738.960342192844],
                        [39692.5243987226],
                        [-59204.10230469701],
                        [-45988.23493249564],
                        [30220.70649401814]
                        ])
    y_re_pred = np.flip(y_pred, 0)
    print('Flip bitches')
    print(y_re_pred)
    #reshape y_pred
    y_pred = y_pred.reshape(y_pred.shape[0], 1, y_pred.shape[1])
    #rebuild test set for inverse transform
    pred_test_set = []
    for index in range(0,len(y_pred)):
        # print (np.concatenate([y_pred[index],X_test[index]],axis=1))
        pred_test_set.append(np.concatenate([y_pred[index],X_test[index]],axis=1))
    #reshape pred_test_set
    pred_test_set = np.array(pred_test_set)
    pred_test_set = pred_test_set.reshape(pred_test_set.shape[0], pred_test_set.shape[2])
    #inverse transform
    scaler = MinMaxScaler(feature_range=(-1,1))
    scaler = scaler.fit(train_set)
    pred_test_set_inverted = pred_test_set#scaler.inverse_transform(pred_test_set)
    print('Hello Y Fucker')
    print(y_pred)
    print('Hello All Fucker')
    print(pred_test_set_inverted)
    #create dataframe that shows the predicted sales
    result_list = []    
    sales_dates = list(df_lstm[-7:].date)
    act_sales = list(df_lstm[-7:].sales)
    for index in range(0,len(pred_test_set_inverted)):
        result_dict = {}
        result_dict['pred_value'] = int(pred_test_set_inverted[index][0] + act_sales[index])
        result_dict['date'] = sales_dates[index+1]
        result_list.append(result_dict)
    df_result = pd.DataFrame(result_list)

    #merge with actual sales dataframe
    df_sales_pred = pd.merge(df_lstm,df_result,on='date',how='left')
    #plot actual and predicted
    plot_data = [
        go.Scatter(
            x=df_sales_pred['date'],
            y=df_sales_pred['sales'],
            name='actual'
     ),
        go.Scatter(
                x=df_sales_pred['date'],
                y=df_sales_pred['pred_value'],
                name='predicted'
    )
    
    ]
    plot_layout = go.Layout(
        title='Sales Prediction'
    )
    fig = go.Figure(data=plot_data, layout=plot_layout)
    pyoff.iplot(fig)

if __name__ == '__main__':
    start_processor()