from time import sleep
from json import dumps
from kafka import KafkaProducer



#producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
producer = KafkaProducer(bootstrap_servers=['10.0.0.11:9092','10.0.0.13:9092','10.0.0.14:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


for e in range(1000):
    data = {'number' : e}
    producer.send('numtest', value=data)
    sleep(5)


