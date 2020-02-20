## import taxi zone info to database, source : https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

import csv
import psycopg2
from psycopg2 import Error

import datetime
from datetime import datetime


conn = psycopg2.connect(host="10.0.0.10",database="db_psql_taxi", user="jelov", password="jelov_psql_pwd")

cur = conn.cursor()

cur.execute("CREATE TABLE taxiZoneInfo (ZoneId integer PRIMARY KEY, Borough text, Zone_Name text, service_zone text);") #only create once

with open('taxi_zone_lookup.csv','r') as f:
    reader=csv.reader(f)
    next(reader)
    for row in reader:
        cur.execute(
        "INSERT INTO taxiZoneInfo VALUES (%s, %s, %s, %s)",
        row
        )
        print(row)

cur.execute("SELECT * FROM taxiZoneInfo;")
cur.fetchone()

conn.commit()
cur.close()
conn.close()

