from termcolor import colored as cp
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import json
import os
import sys
import logging
import numpy as np
from matplotlib import pyplot as plt

#KERAS IMPORTS
import keras
from keras.layers import Dense
from keras.models import Sequential
from keras.optimizers import Adam 
from keras.callbacks import EarlyStopping
from keras.utils import np_utils
from keras.layers import LSTM

import plotly as py
import plotly.offline as pyoff
import plotly.graph_objs as go


CONFIG_PATH = '/Users/konstantingasser/workspaces/vs_code-workspace/uip_sap/project_sourceCode/uip-infrastructure/ml-models/engine/.config/processing.config.json'
"""
function for deveolpent in order to see the current DataFrame status
prints:: df.head(), df.shape
"""
def _get_df_info(df):
    print(cp('DataFrame info:', 'green'))
    print(cp(df.head(),'green'))
    print(cp(df.shape, 'green'))

"""
function reads selected column from the json file:
dataFrame-Path -> returns a string path of the file to load
categoryNumeric-cols -> returns a list of columns with both categorical and numerical values
numeric-cols -> returns a list of columns with only numeric values
raises error if reading fails.
return:: conf:dict{}
"""
def _get_json_config():
    try:
        with open(CONFIG_PATH, 'r') as config:
            conf = json.load(config)
            return conf
    except Exception as json_read_error:
        logging.error('processing.config.json failed to load', exc_info=True)
        print(cp(f'log[error]: processing.config.json failed to load \n{ json_read_error }', 'red'))

"""
functio to retrieve value from loaded json config
param:: key:string, config:dict{}
return:: value:string
"""
def _get_value_from_config(key, config):
    try:
        return config['lstm-script'][key]
    except KeyError as key_error:
        logging.error('key not in configuration.', exc_info=True)
        print(cp(f'log[error]: key not in configuration dictonary \n{ key_error }', 'red'))


"""
function using the Standard Scaler class from sklean.processing
scales the columns in range(-1,1)
return:: train_set:numpy.array, test_set:numpy.array
"""
def _scale_features(train, test):
    try:

        scaler = MinMaxScaler(feature_range=(-1,1))
        scaler = scaler.fit(train)
        
        print(f'Max: {scaler.data_max_}')
        print(f'Min: {scaler.data_min_}')

        train_set = train.reshape(train.shape[0], train.shape[1])
        test_set = test.reshape(test.shape[0], test.shape[1])
        train_scaled = scaler.transform(train_set)
        test_scaled = scaler.transform(test_set)

        return train_scaled, test_scaled

    except Exception as lib_error:
        logging.error('StandardScaler failed to scale features', exc_info=True)
        print(cp(f'log[error]: StandardScaler failed to scale features \n{ lib_error }', 'red'))

"""
function calling the train function in the according ml-model script
param:: features:pandas.DataFrame, target:pandas.DataFrame
return:: NONE
"""

"""
function computing the difference between the sale in row n and row n+1
param:: df:pandas.DataFrame, target:String
return:: df::pandas.DataFrame
"""
def _compute_difference(df, target):
    df['prev_sales'] = df[target].shift(1)
    df = df.dropna()
    df['difference'] = (df[target] - df['prev_sales'])
    return df    

"""
function creating n columns each refering to a shifted target of {n}: needed to make forecast
range(1,13) -> lag_1 - lag_12 => for each month
param:: df:pandas.DatFrame
return super_df:pandas.DataFrame
"""
def _get_supervised_df(prev_df):
    super_df = prev_df.drop(['prev_sales'], axis=1)
    for n in range(1,13):
        f_name = 'lag_' + str(n)
        super_df[f_name] = super_df['difference'].shift(n)
    super_df = super_df.dropna().reset_index(drop=True)
    return super_df

"""
function returns eighter a train or a test set(s)
parra:: data_set:numpy.array
return:: X_,y_,X1_,y1_:numpy.array
"""
def _train_test_set(train_set, test_set):
    X_train, y_train = train_set[:, 1:], train_set[:, 0:1]
    X_train = X_train.reshape(X_train.shape[0], 1, X_train.shape[1])

    X_test, y_test = test_set[:, 1:], test_set[:, 0:1]
    
    X_test = X_test.reshape(X_test.shape[0], 1, X_test.shape[1])
    
    return X_train, y_train, X_test, y_test
    

"""
function to train the LSTM model and save the model.h5 file for later usage
para:: X_train:numpy.array, y_train:numpy.array
"""
def _train_the_beast(X, y):
   try:
        model = Sequential()
        model.add(
            LSTM(4, batch_input_shape=(1, X.shape[1], X.shape[2]),
            stateful=True)
            )

        model.add(Dense(1))
        model.compile(
            loss='mean_squared_error',
            optimizer='adam'
            )

        model.fit(
            x=X,
            y=y,
            nb_epoch=100,
            batch_size=1,
            verbose=1,
            shuffle=False
            ) 
        return model
   except Exception as keras_error:
        logging.error('Keras LSTM model failed to while fitting', exc_info=True)
        print(cp(f'Keras LSTM model failed to while fitting \n{ keras_error }', 'red'))
        return keras_error
"""
function fetching the data and piping it into a dataframe, get cols in focus
df -> holds the whole data set in a pandas DataFrame
cate_num_cols, num_cols -> each is a list holding column names selected in the json file
"""
def start_processor():
    
    ##loading of the data requirements and data frame ####################################
    config     = _get_json_config()
    file_path  = _get_value_from_config(key='dataFrame-path', config=config)
    target     = _get_value_from_config(key='target-col', config=config)
    group_date = _get_value_from_config(key='date-col', config=config)
    group_by   = _get_value_from_config(key='group-by', config=config)
    
    df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=',') # loading of the data into a pandas DataFrame
    ######################################################################################

    ##DataFrame segregation into the DataFrames described by the cat_num_cols & num_cols##
    num_df = df[[group_date, target]].copy()
    num_df[group_date] = pd.to_datetime(num_df[group_date])
    ######################################################################################

    ##indexing and grouping of the data for numeric cols##################################
    # NOTE: month == 'm' week == 'W'(day a week start can be specified with i.e 'W-MON' -> start monday) day == 'D'
    df_sales = num_df.copy()
    df_sales = df_sales.rename(columns={'Order Date': 'date', 'Sales': 'sales'})
    #represent month in date field as its first day
    df_sales['date'] = df_sales['date'].dt.year.astype('str') + '-' + df_sales['date'].dt.month.astype('str') + '-01'
    df_sales['date'] = pd.to_datetime(df_sales['date'])

    df_sales = df_sales.groupby('date').sales.sum().reset_index()
    _get_df_info(df_sales)

    ##for LSTM we need the difference between the instances in the dataframe###############
    #computing the differenc onto a new Data Frame
    # num_diff = _compute_difference(num_aggr, target)
    df_diff = df_sales.copy()
    #add previous sales to the next row
    df_diff['prev_sales'] = df_diff['sales'].shift(1)
    #drop the null values and calculate the difference
    print(df_diff.head())
    print(df_diff.shape)
    df_diff = df_diff.dropna()
    print(f'shape01 {df_diff.shape}')
    df_diff['diff'] = (df_diff['sales'] - df_diff['prev_sales'])
 

    ##creating the feature dataframe. shifting of each prev/post sales in a column lag_n
    # supervised_df = _get_supervised_df(num_diff)
    df_supervised = df_diff.drop(['prev_sales'],axis=1)
    #adding lags
    for inc in range(1,12):
        field_name = 'lag_' + str(inc)
        df_supervised[field_name] = df_supervised['diff'].shift(inc)
        print(inc)
        print(df_supervised.head(25))
    #drop null values
    df_supervised = df_supervised.dropna().reset_index(drop=True)
    print(f'super:{df_supervised.shape}')
    ##split data frame in test and split set train_set, test_set:numpy.array
    # NOTE: set will be split in 6 month each!
    df_model = df_supervised.drop(['sales','date'],axis=1)
    #split train and test set
    train_set, test_set = df_model[0:-6].values, df_model[-6:].values
    print(f'shape {train_set.shape}')
    #apply Min Max Scaler
    scaler = MinMaxScaler(feature_range=(-1, 1))
    scaler = scaler.fit(train_set)
    # reshape training set
    train_set = train_set.reshape(train_set.shape[0], train_set.shape[1])
    train_set_scaled = scaler.transform(train_set)
    # reshape test set
    test_set = test_set.reshape(test_set.shape[0], test_set.shape[1])
    test_set_scaled = scaler.transform(test_set)
    print(test_set_scaled.shape)
    X_train, y_train = train_set_scaled[:, 1:], train_set_scaled[:, 0:1]
    X_train = X_train.reshape(X_train.shape[0], 1, X_train.shape[1])
    X_test, y_test = test_set_scaled[:, 1:], test_set_scaled[:, 0:1]
    X_test = X_test.reshape(X_test.shape[0], 1, X_test.shape[1])

    print(f'X_test: {X_test.shape}')
    model = Sequential()
    model.add(LSTM(4, batch_input_shape=(1, X_train.shape[1], X_train.shape[2]), stateful=True))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error', optimizer='adam')
    model.fit(X_train, y_train, nb_epoch=120, batch_size=1, verbose=1, shuffle=False)
    np.set_printoptions(suppress=True)
    
    y_pred = model.predict(X_test,batch_size=1)
    print('Hello Motherfucker')
    print(y_pred)

    #reshape y_pred
    y_proof = np.array([[0.0266],[0.2377],[-0.0429],[0.0669]])
    y_pred = y_pred.reshape(y_pred.shape[0], 1, y_pred.shape[1])
    y_pred = y_proof.reshape(y_proof.shape[0], 1, y_proof.shape[1])
    #rebuild test set for inverse transform
    pred_test_set = []
    for index in range(0,len(y_pred)):
        pred_test_set.append(np.concatenate([y_pred[index],X_test[index]],axis=1))
    #reshape pred_test_set
    pred_test_set = np.array(pred_test_set)
    pred_test_set = pred_test_set.reshape(pred_test_set.shape[0], pred_test_set.shape[2])
    #inverse transform
    pred_test_set_inverted = scaler.inverse_transform(pred_test_set)
    # proof_inv = scaler.inverse_transform(y_proof)
    # print(f'PROOFOOOOFFF: {proof_inv}')
    #create dataframe that shows the predicted sales
    result_list = []
    sales_dates = list(df_sales[-7:].date)
    act_sales = list(df_sales[-7:].sales)
    for index in range(0,len(pred_test_set_inverted)):
        result_dict = {}
        result_dict['pred_value'] = int(pred_test_set_inverted[index][0] + act_sales[index])
        result_dict['date'] = sales_dates[index+1]
        result_list.append(result_dict)
    df_result = pd.DataFrame(result_list)

    df_sales_pred = pd.merge(df_sales,df_result,on='date',how='left')

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





    # y_pred = y_pred.reshape(y_pred.shape[0], 1, y_pred.shape[1])
    # pred_test_set = []
    # for index in range(0,len(y_pred)):
    #     pred_test_set.append(np.concatenate([y_pred[index],X_test[index]],axis=1))

    # pred_test_set = np.array(pred_test_set)
    # pred_test_set = pred_test_set.reshape(pred_test_set.shape[0], pred_test_set.shape[2])

    # scaler = MinMaxScaler(feature_range=(-1,1))
    # scaler = scaler.fit(train_set)
    # pred_test_set_inverted = scaler.inverse_transform(pred_test_set)

    # result_list = []
    # sales_dates = list(num_df[-7:].index)  
    # act_sales = list(num_df[-7:].Sales)
    # for index in range(0,len(pred_test_set_inverted)):
    #     result_dict = {}
    #     result_dict['pred_value'] = int(pred_test_set_inverted[index][0] + act_sales[index])
    #     result_dict['order_date'] = sales_dates[index+1]
    #     result_list.append(result_dict)
    #     df_result = pd.DataFrame(result_list)
    
    # print(df_result.head())
    # num_aggr.insert(0, 'Order Date', num_aggr.index.values, allow_duplicates=True)
    # num_aggr['Order Date'] = pd.to_datetime(num_aggr['Order Date'])
    # num_aggr.index.names = ['penis']
    # num_aggr = num_aggr.reset_index()
    # num_aggr = num_aggr.drop('penis', 1)
    # num_aggr.columns = num_aggr.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')','')
    # print(num_aggr.info())
    
    # num_aggr = num_aggr.dropna()
    # num_aggr = num_aggr.drop(['prev_sales'], 1)
    # _get_df_info(num_aggr)
    # df_sales_pred = df_result.copy()
    # print(df_sales_pred)
    #plot actual and predicted
    # plot_data = [
    #     go.Scatter(
    #         x=df_sales_pred['order_date'],
    #         y=num_aggr['sales'],
    #         name='actual'
    #     ),
    #         go.Scatter(
    #         x=df_sales_pred['order_date'],
    #         y=df_sales_pred['pred_value'],
    #         name='predicted'
    #     )]
    # plot_layout = go.Layout(
    #     title='Sales Prediction'
    # )
    # fig = go.Figure(data=plot_data, layout=plot_layout)
    # pyoff.iplot(fig)
    # model.save('LSTMmodel.h5')

    #TODO: returned model.h5 from trainig has to be saved in a directory

if __name__ == '__main__':
    # config loggin properties
    logging.basicConfig(filename='processing-logs.log', 
                        filemode='a', 
                        format='%(asctime)s:%(levelname)s-%(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    start_processor()