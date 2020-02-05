from kafka import KafkaConsumer
#from pymongo import MongoClient
from json import loads


topic='TaxiData'

consumer = KafkaConsumer(
     topic,
     bootstrap_servers=['10.0.0.11:9092','10.0.0.13:9092','10.0.0.14:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
#     value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

for message in consumer:
    message = message.value
    print(message)
