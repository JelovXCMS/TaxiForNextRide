## from basic module usage

import psycopg2
from psycopg2 import Error

import datetime


conn = psycopg2.connect(host="10.0.0.10",database="my_db", user="db_test", password="db_test_pwd")
#conn = psycopg2.connect("host='10.0.0.10' dbname='my_db'  user='db_test' password='db_test_pwd'")

cur = conn.cursor()

# Execute a command: this creates a new table
cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")

cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)",
        (100, "abc'def"))

cur.execute("SELECT * FROM test;")
cur.fetchone()

conn.commit()
cur.close()
conn.close()



#create_table_query = '''CREATE TABLE mobile
#          (ID INT PRIMARY KEY     NOT NULL,
#          MODEL           TEXT    NOT NULL,
#          PRICE         REAL); '''

#cursor.execute(create_table_query)
#conn.commit()



#cursor.execute("SELECT * FROM role")
