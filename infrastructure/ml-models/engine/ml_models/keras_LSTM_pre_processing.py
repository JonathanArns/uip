from termcolor import colored as cp
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import json
import os
import sys
import logging
import numpy as np
from matplotlib import pyplot as plt


CONFIG_PATH = 'infrastructure/ml-models/engine/ml_models/.config/processing_config.json'
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
        print(cp('log[error]: processing.config.json failed to load \n{ json_read_error }', 'red'))

"""
functio to retrieve value from loaded json config
param:: key:string, config:dict{}
return:: value:string
"""
def _get_value_from_config(key, config):
    try:
        return config['regression-script'][key]
    except KeyError as key_error:
        logging.error('key not in configuration.', exc_info=True)
        print(cp('log[error]: key not in configuration dictonary \n{ key_error }', 'red'))

"""
function returning to DataFrames described by the parameters cols_1[], cols_2[]
return:: df_with_cols_:pandas.DataFrame
"""
def _seperate_df(df_cols_, _df):
    return  _df[df_cols_].copy()


"""
function using the Standard Scaler class from sklean.processing
scales the columns in range(-1,1)
return:: train_set:numpy.array, test_set:numpy.array
"""
def _scale_features(train, test):
    try:
        scaler = MinMaxScaler(feature_range=(-1,1))
        scaler = scaler.fit(train)

        train_set = train.reshape(train.shape[0], train.shape[1])
        test_set = test.reshape(test.shape[0], test.shape[1])
        train_scaled = scaler.transform(train_set)
        test_scaled = scaler.transform(test_set)

        return train_scaled, test_scaled

    except Exception as lib_error:
        logging.error('StandardScaler failed to scale features', exc_info=True)
        print(cp('log[error]: StandardScaler failed to scale features \n{ lib_error }', 'red'))

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
    _get_df_info(df)
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


def _train_the_beast(feature, target):
    #TODO: Call training function from ml-model script
    pass
"""
function fetching the data and piping it into a dataframe, get cols in focus
df -> holds the whole data set in a pandas DataFrame
cate_num_cols, num_cols -> each is a list holding column names selected in the json file
"""
def start_processor():

    ##loading of the data requirements and data frame ####################################
    config = _get_json_config()
    file_path = _get_value_from_config(key='dataFrame-path', config=config)
    num_cols  = _get_value_from_config(key='numeric-cols', config=config)
    group_date = num_cols[0] # !IMPORTANT: column with date has to be at index[0] in the json file!
    group_by  = _get_value_from_config(key='group-by', config=config)
    feature   = _get_value_from_config(key='feature-col', config=config)
    target    = _get_value_from_config(key='target-col', config=config)
    df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=',') # loading of the data into a pandas DataFrame
    ######################################################################################

    ##DataFrame segregation into the DataFrames described by the cat_num_cols & num_cols##
    num_df = _seperate_df(num_cols, df)
    num_df = num_df.loc[:,(group_date, target)]
    num_df[group_date] = pd.to_datetime(num_df[group_date])
    _get_df_info(num_df.head())
    ######################################################################################

    ##indexing and grouping of the data for numeric cols##################################
    # NOTE: month == 'm' week == 'W'(day a week start can be specified with i.e 'W-MON' -> start monday) day == 'D'
    num_aggr = num_df.copy()
    num_aggr = num_aggr.set_index(group_date).resample(group_by).sum()
    _get_df_info(num_aggr)

    ##for LSTM we need the difference between the instances in the dataframe###############
    #computing the differenc onto a new Data Frame
    num_diff = _compute_difference(num_aggr, target)
    _get_df_info(num_diff)

    ##creating the feature dataframe. shifting of each prev/post sales in a column lag_n
    supervised_df = _get_supervised_df(num_diff)
    _get_df_info(supervised_df)

    ##split data frame in test and split set train_set, test_set:numpy.array
    # NOTE: set will be split in 6 month each!
    feature_super = supervised_df.drop([target], axis=1)
    _get_df_info(feature_super)
    train_set, test_set = feature_super[0:-6].values, feature_super[-6:].values
    train_set_scaled, test_set_scaled = _scale_features(train_set, test_set)

    X_train, y_train = train_set_scaled[:, 1:], train_set_scaled[:, 0:1]
    X_train = X_train.reshape(X_train.shape[0], 1, X_train.shape[1])
    X_test, y_test = test_set_scaled[:, 1:], test_set_scaled[:, 0:1]
    X_test = X_test.reshape(X_test.shape[0], 1, X_test.shape[1])

    #TODO: build LSTM model


    #TODO: returned model.h5 from trainig has to be saved in a directory

if __name__ == '__main__':
    # config loggin properties
    logging.basicConfig(filename='processing-logs.log',
                        filemode='a',
                        format='%(asctime)s:%(levelname)s-%(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    start_processor()
