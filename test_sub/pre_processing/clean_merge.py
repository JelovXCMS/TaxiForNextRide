import boto3
import os
import sys
import pandas as pd


client = boto3.client('s3')
bucket = "s3nyctaxi"

year="2019"
month="04"
filepath = ""
filename1 = "yellow_tripdata_{}-{}.csv".format(year,month)
filename2 = "green_tripdata_{}-{}.csv".format(year,month)

obj1 = client.get_object(Bucket=bucket, Key=filepath+filename1)
obj2 = client.get_object(Bucket=bucket, Key=filepath+filename2)
df1=pd.read_csv(obj1['Body'],usecols=["tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID"])
df2=pd.read_csv(obj2['Body'],usecols=["lpep_pickup_datetime","lpep_dropoff_datetime","PULocationID","DOLocationID"])
df2.columns=["tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID"]


frames=[df1,df2]

df=pd.concat(frames,ignore_index=True,sort=False)

df['tpep_pickup_datetime_temp']=pd.to_datetime(df['tpep_pickup_datetime'],format="%Y-%m-%d %H:%M:%S")
df['tpep_dropoff_datetime_temp']=pd.to_datetime(df['tpep_dropoff_datetime'],format="%Y-%m-%d %H:%M:%S")
df['PickYear']=df['tpep_pickup_datetime_temp'].dt.year
df['PickMonth']=df['tpep_pickup_datetime_temp'].dt.month

df['TravelTime']=(df.tpep_dropoff_datetime_temp-df.tpep_pickup_datetime_temp).astype('timedelta64[s]')
df0=df.query('`TravelTime` >60 & `TravelTime`<7200 & PickYear == @year & PickMonth == @month ')
df0_save=df0.drop(['PickYear','PickMonth','TravelTime','tpep_pickup_datetime_temp','tpep_dropoff_datetime_temp'],axis=1).sort_values(by=['tpep_pickup_datetime'])

filenameOut = "YGMergeClean_tripdata_{}-{}.csv".format(year,month)
df0_save.to_csv(filenameOut)

#upload to s3

resource = boto3.resource('s3')
my_bucket=resource.Bucket(bucket)
my_bucket.upload_file(filenameOut,Key=filenameOut)

#delete local

os.remove(filenameOut)
