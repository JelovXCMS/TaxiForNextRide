# from basic module usage

import psycopg2
from psycopg2 import Error

import datetime
from datetime import datetime


#conn = psycopg2.connect(host="10.0.0.10",database="my_db", user="db_test", password="db_test_pwd")
conn = psycopg2.connect(host="10.0.0.10",database="db_psql_taxi", user="jelov", password="jelov_psql_pwd")
#cur = conn.cursor()#conn = psycopg2.connect("host='10.0.0.10' dbname='my_db'  user='db_test' password='db_test_pwd'")

cur = conn.cursor()

# Execute a command: this creates a new table
#cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")

#cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)",
#        (100, "abc'def"))

PickDT=datetime.strptime("2020-01-02 00:16:45", '%Y-%m-%d %H:%M:%S')
wait = 23333 
zoneid= "1"

#cur.execute("CREATE TABLE taxi (id serial PRIMARY KEY, dat timestamp, waitT integer);")
cur.execute("CREATE TABLE taxi_table (id serial PRIMARY KEY, dat timestamp, waitT integer, zoneId integer);") #only create once
cur.execute("INSERT INTO taxi_table (dat, waitT, zoneId) VALUES (%s, %s, %s)",
        (PickDT, wait, int(zoneid)))


cur.execute("SELECT * FROM taxi_table;")
cur.fetchone()

#conn.commit()
cur.close()
conn.close()



#create_table_query = '''CREATE TABLE mobile
#          (ID INT PRIMARY KEY     NOT NULL,
#          MODEL           TEXT    NOT NULL,
#          PRICE         REAL); '''

#cursor.execute(create_table_query)
#conn.commit()



#cursor.execute("SELECT * FROM role")
