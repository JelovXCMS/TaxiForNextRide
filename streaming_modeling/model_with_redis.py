from kafka import KafkaConsumer
from json import loads

import psycopg2
from psycopg2 import Error


from datetime import datetime
from datetime import timedelta
import redis
import time

## postgresql 
conn = psycopg2.connect(host="10.0.0.10",database="db_psql_taxi", user="jelov", password="jelov_psql_pwd")
cur = conn.cursor()

#cur.execute("CREATE TABLE taxi_table (id serial PRIMARY KEY, dat timestamp, waitT integer, zoneId integer);") #only create once


topic='TaxiData'

maxZoneId=265

consumer = KafkaConsumer(
     topic,
     bootstrap_servers=['10.0.0.11:9092','10.0.0.13:9092','10.0.0.14:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
    )

def getTrip(message_str_L):
    PickDT=datetime.strptime(  message_str_L[1] , '%Y-%m-%d %H:%M:%S')
    DropDT=datetime.strptime(  message_str_L[2] , '%Y-%m-%d %H:%M:%S')
    PickLId=message_str_L[3]
    DropLId=message_str_L[4]
    TravelTime = (DropDT - PickDT).total_seconds()
    PickTimeAbs = PickDT.hour*60*60+PickDT.minute*60+PickDT.second
    return PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs


### pre-skip , skip 0-10 hour
PickTimeAbs = 0
BeginTime = 10*3600 # start from 10
setUpTime=0.75*3600 + BeginTime # set up taxi initial  distribution period
EndTime=19*3600
taxiCount=0
N_trip=0

r1=redis.Redis()
r1.flushdb() ## clean db

ZoneWaitTime=[-1]*(maxZoneId+1)

currentDay=0

for message in consumer :
    message = message.value
    print(message)
    message_str=message.decode("utf-8")
    #print(message_str.split(',')[0])
    message_strL=message_str.split(',')
    # message_strL [1] : pickUpTime , [2] : dropOffTime , [3] : pickUpLId, [4] : dropOffLId
    if message_strL[0] == "" :  ## skip title line message 
        print("title")
        continue
    PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs = getTrip(message_strL)

    if PickDT.day != currentDay : ## new day
        taxiCount=0
        N_trip=0
        r1.flushdb() ## clean db
        print("currentDay")
        print(currentDay)
        print(ZoneWaitTime)
        ZoneWaitTime=[-1]*(maxZoneId+1)
        currentDay = PickDT.day

    if PickTimeAbs < BeginTime :
        continue


    if PickTimeAbs < setUpTime : ## init taxi distribution
        #print("init taxi distribution")
        r1.zadd(PickLId,{"taxi:{}".format(taxiCount+1):setUpTime-1})
        taxiCount +=1
        continue
  

    ## start taxi update
    ## trips request added into list

 ##   if PickTimeAbs > EndTime:
 ##       continue

    if PickTimeAbs < EndTime :
        print("taking trip request")
        r1.rpush("re_lFromId",PickLId)  ## three lists for trip requests 
        r1.rpush("re_lToId",DropLId)
        r1.rpush("re_lTime",TravelTime)
        N_trip +=1

    ## update taxi distribution and match with request
    currentTime = PickTimeAbs
    currentTimeStamp = PickDT
    if N_trip == 10 :   ## choose a reasonalble number to reduce operations
        ## update taxi from "future arrive table"
        print("updating taxi")
        for i in range(maxZoneId) :
            while True :
                rpf=r1.zpopmin("F{}".format(i+1))
                if rpf == [] :  ## no taxi
                    break
                if rpf[0][1]>currentTime :  ## taxi hasn't arrived , put it back to future
                    r1.zadd("F{}".format(i+1),{rpf[0][0]:rpf[0][1]})
                    break
                r1.zadd("{}".format(i+1),{rpf[0][0]:rpf[0][1]}) # put taxi into wating queue


        ## match request with taxi queue, update waiting time, send match to future

        dummyI=0
        len_l=r1.llen("re_lFromId")
        while dummyI < len_l : ## iterate request table
            PickLId = r1.lpop("re_lFromId").decode("utf-8")
            DropLId = r1.lpop("re_lToId").decode("utf-8")
            TravelTime = float(r1.lpop("re_lTime").decode("utf-8"))

            rp=r1.zpopmin(PickLId)      ## check taxi match request
            if rp == [] : ## not matched , send request back to head of queue
                r1.rpush("re_lFromId",PickLId)
                r1.rpush("re_lToId",DropLId)
                r1.rpush("re_lTime",TravelTime)
            else :  ## matched , update waiting time & future table
                print("taxi matched")
                len_l -=1
                WaitTimeS=currentTime-rp[0][1]
                if WaitTimeS < 0 : ## exception here
                    print("negative WaitTime")
                r1.zadd("F"+DropLId,{rp[0][0]:currentTime+TravelTime}) ## update zone taxi queue to future
                ZoneWaitTime[int(PickLId)]=WaitTimeS ## need to migirate to sql later
                cur.execute("INSERT INTO taxi_table (dat, waitT, zoneId) VALUES (%s, %s, %s)",
                    (currentTimeStamp, WaitTimeS, int(PickLId)))

            dummyI += 1

        ## balance taxi v.s request
        while r1.llen("re_lFromId") >80 :
            PickLId = r1.lpop("re_lFromId").decode("utf-8")
            DropLId = r1.lpop("re_lToId").decode("utf-8")
            TravelTime = float(r1.lpop("re_lTime").decode("utf-8"))            

        
        N_trip=0  ## reset N_trip   

        conn.commit() ## commit to postgresql


#    if PickTimeAbs > EndTime:
#        print(ZoneWaitTime)

## close connection to postgresql
cur.close()
conn.close()



