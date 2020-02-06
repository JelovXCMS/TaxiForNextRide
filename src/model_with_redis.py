from kafka import KafkaConsumer
from json import loads

topic='TaxiData'

consumer = KafkaConsumer(
     topic,
     bootstrap_servers=['10.0.0.11:9092','10.0.0.13:9092','10.0.0.14:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
    )

def getTrip(message_strL_L):
    PickDT=datetime.strptime(  message_strL[1] , '%Y-%m-%d %H:%M:%S')
    DropDT=datetime.strptime(  message_strL[2] , '%Y-%m-%d %H:%M:%S')
    PickLId=message_strL[3]
    DropLId=message_strL[4]
    TravelTime = (DropDT - PickDT).total_seconds()
    PickTimeAbs = PickDT.hour*60*60+PickDT.minute*60+PickDT.second
    return PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs




for message in consumer :
    message = message.value
    print(message)
    print(type(message))
    message_str=message.decode("utf-8")
    print(message_str.split(',')[0])
    message_strL=message_str.split(',')
    # message_strL [1] : pickUpTime , [2] : dropOffTime , [3] : pickUpLId, [4] : dropOffLId
    if message_strL[0] == "" :  ## skip title line message 
        print("title")
        continue



    print("hello")
    break


