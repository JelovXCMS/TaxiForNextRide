from kafka import KafkaProducer
from smart_open import smart_open
from time import sleep


producer = KafkaProducer(bootstrap_servers=['10.0.0.11:9092','10.0.0.13:9092','10.0.0.14:9092'])


topic='TaxiData'
fpath='s3://s3nyctaxi/YGMergeClean_tripdata_2019-06.csv'

count=0
for msg in smart_open(fpath):
    producer.send(topic,msg)
    producer.flush()

    count+=1
    if count ==20 :    ## take rest for several messages
        sleep(2)
        count = 0 

