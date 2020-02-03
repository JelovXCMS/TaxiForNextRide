## input test file  yellow_tripdata_2019-06.csv

import redis

r= redis.Redis()

r3=redis.Redis()
r3.zadd("Zone1Arr",{"taxi:01":151})
r3.zadd("Zone1Arr",{"taxi:02":154})
r3.zadd("Zone1Arr",{"taxi:03":138})

print(r3.zscan("Zone1Arr",0))
r3p=r3.zpopmin("Zone1Arr") ## r3p is a list with only one entry

if r3.zpopmin("Zone1Arr") == [] :
  print("empty")

# using pandas as test data source


import pandas as pd
import numpy as np

pd.set_option('display.max_rows',30)
pd.set_option('display.max_columns',None)


df=pd.read_csv('yellow_tripdata_2019-0601.csv',usecols=["tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID"])
df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'],format="%Y-%m-%d %H:%M:%S")
df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'],format="%Y-%m-%d %H:%M:%S")
df['PickTime']=pd.to_datetime(df['tpep_pickup_datetime'],format="%Y-%m-%d %H:%M:%S").dt.time
df['PickHour']=pd.to_datetime(df['tpep_pickup_datetime'],format="%Y-%m-%d %H:%M:%S").dt.hour
#df['PickDay']=pd.to_datetime(df['tpep_pickup_datetime'],format="%Y-%m-%d %H:%M:%S").dt.day
df['DropTime']=pd.to_datetime(df['tpep_dropoff_datetime'],format="%Y-%m-%d %H:%M:%S").dt.time
df['DropHour']=pd.to_datetime(df['tpep_dropoff_datetime'],format="%Y-%m-%d %H:%M:%S").dt.hour

# calcualte duration and assigne the result in format of sec
df['TravelTime']=(df.tpep_dropoff_datetime-df.tpep_pickup_datetime).astype('timedelta64[s]')
#df0=df[df['TravelTime']<0]
df0=df.query('`TravelTime` >60 & `TravelTime`<7200 ')

test=df.query('PickHour = 9 ').count()

test=df0.query('`PickHour` == 9 ').count()[0]


# count number in each hour

#clean those TravelTime <0 or >7200 (2hr) mostly from wrong input



'''
import csv

with open('yellow_tripdata_2019-0601.csv') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in spamreader:
        print ', '.join(row)
'''

