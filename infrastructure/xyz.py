#!/usr/bin/python

hostname = 'localhost'
username = 'kafka_connect'
password = 'kafka_connect'
database = 'kafka_connect'

# Simple routine to run a query on a database and print the results:
def doQuery( conn ) :
    cur = conn.cursor()

    return cur.execute( "Select * from 'lstm_results'" )


import psycopg2
myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
df = pd.read_sql_query(doQuery( myConnection ))
df.to_csv(r'Path where you want to store the exported CSV file\File Name.csv')
myConnection.close()
