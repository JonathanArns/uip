#!/usr/bin/python
import pandas as pd
import psycopg2

hostname = 'localhost'
username = 'kafka_connect'
password = 'kafka_connect'
database = 'kafka_connect'

myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
df = pd.read_sql_query("SELECT * FROM lstm_results ORDER BY date ASC", myConnection)
df.to_csv('lstm_results.csv', index=None)
myConnection.close()
