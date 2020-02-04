from time import sleep
from json import dumps
from kafka import KafkaProducer
from smart_open import smart_open
#from smart_open import open
import sys

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
#                         value_serializer=lambda x: 
#                         dumps(x).encode('utf-8'))


fpath='s3://s3nyctaxi/yellow_tripdata_2018-06.csv'
for msg in smart_open(fpath):
    #print(repr(msg))
    #break
    producer.send('numtest',msg)
    producer.flush()
# for e in range(1000):
#     data = {'number' : e}
#     producer.send('numtest', value=data)
#     sleep(5)


