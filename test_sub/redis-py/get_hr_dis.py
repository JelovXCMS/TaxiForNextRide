import pandas as pd
import numpy as np

pd.set_option('display.max_rows',30)
pd.set_option('display.max_columns',None)

df=pd.read_csv('yellow_tripdata_2019-06.csv',usecols=["tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID"])
#df=pd.read_csv('yellow_tripdata_2019-0601.csv',usecols=["tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID"])

# pre-clean

df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'],format="%Y-%m-%d %H:%M:%S")
df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'],format="%Y-%m-%d %H:%M:%S")
df['PickTime']=pd.to_datetime(df['tpep_pickup_datetime'],format="%Y-%m-%d %H:%M:%S").dt.time
df['PickHour']=pd.to_datetime(df['tpep_pickup_datetime'],format="%Y-%m-%d %H:%M:%S").dt.hour
df['PickDay']=pd.to_datetime(df['tpep_pickup_datetime'],format="%Y-%m-%d %H:%M:%S").dt.day
df['DropTime']=pd.to_datetime(df['tpep_dropoff_datetime'],format="%Y-%m-%d %H:%M:%S").dt.time
df['DropHour']=pd.to_datetime(df['tpep_dropoff_datetime'],format="%Y-%m-%d %H:%M:%S").dt.hour


df['TravelTime']=(df.tpep_dropoff_datetime-df.tpep_pickup_datetime).astype('timedelta64[s]')

df0=df.query('`TravelTime` >60 & `TravelTime`<7200 ')

##

df601=df0.query('`PickDay` == 1')
l601=[]

for x in range(0, 23):
    l601.append(df601.query("PickHour == {} ".format(x)).count()[0])


df602=df0.query('`PickDay` == 2')
l602=[]

for x in range(0, 23):
    l602.append(df602.query("PickHour == {} ".format(x)).count()[0])

df603=df0.query('`PickDay` == 3')
l603=[]

for x in range(0, 23):
    l603.append(df603.query("PickHour == {} ".format(x)).count()[0])


df604=df0.query('`PickDay` == 4')
l604=[]

for x in range(0, 23):
    l604.append(df604.query("PickHour == {} ".format(x)).count()[0])


df605=df0.query('`PickDay` == 5')
l605=[]

for x in range(0, 23):
    l605.append(df605.query("PickHour == {} ".format(x)).count()[0])


ln=[l601,l602,l603,l604,l605]
df_dis = pd.DataFrame(ln, columns =[ '00' , '01', '02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22'], dtype = float)

df_dis.to_csv("0601_0605_hour_requests.csv")

# build from 604 data , do pre-clean
# df604_clean=df604.drop(['B', 'C'], axis=1)

df604_clean=df604.drop(['PickTime','PickHour','PickDay','DropTime','DropHour'],axis=1).sort_values(by=['tpep_pickup_datetime'])

df604_clean.to_csv("0604_clean_request_test.csv")


#df604['PickTimeS']=(df604.tpep_dropoff_datetime- pandas.Timestamp('2014-01-24 13:03:12.050000')).astype('timedelta64[s]')
#df604['PickTimeS']=(df604.tpep_dropoff_datetime).astype('timedelta64[s]')

