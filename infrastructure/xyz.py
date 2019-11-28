#!/usr/bin/python

hostname = 'localhost'
username = 'kafka_connect'
password = 'kafka_connect'
database = 'kafka_connect'

# Simple routine to run a query on a database and print the results:
def doQuery( conn ) :
    cur = conn.cursor()

    cur.execute( "COPY lstm_results TO 'db.csv' DELIMITER ';' CSV HEADER;" )


print "Using psycopg2…"
import psycopg2
myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
doQuery( myConnection )
myConnection.close()

print "Using PyGreSQL…"
import pgdb
myConnection = pgdb.connect( host=hostname, user=username, password=password, database=database )
doQuery( myConnection)
myConnection.close()
