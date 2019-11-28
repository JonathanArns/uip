#!/usr/bin/python

hostname = 'localhost'
username = 'kafka_connect'
password = 'kafka_connect'
database = 'kafka_connect'

# Simple routine to run a query on a database and print the results:
def doQuery( conn ) :
    cur = conn.cursor()

    cur.execute( "COPY lstm_results TO '/var/lib/postgresql/data/export.csv' DELIMITER ';' CSV HEADER;" )

import psycopg2
myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
doQuery( myConnection )
myConnection.close()
