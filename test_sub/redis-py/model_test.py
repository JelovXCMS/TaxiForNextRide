## input : cleaned csv file of one day yellow taxi (readin as line by line)
## use redis as database
## 

import redis
import time

infile=open("0604_clean_request_test.csv", "r")
Trips = infile.readlines() 
nTrip = len(Trips)
count=1;  ## count 0 is the head


maxZoneId=265  ##  nyc taxi zone from 1-265

#timedelta.total_seconds()# https://www.journaldev.com/23365/python-string-to-datetime-strptime
# from datetime import timedelta

from datetime import datetime
from datetime import timedelta

datetime_str = '09/19/18 13:55:26'
datetime_object = datetime.strptime(datetime_str, '%m/%d/%y %H:%M:%S')

datetime_str2 = '09/19/18 13:57:20'
datetime_object2 = datetime.strptime(datetime_str2, '%m/%d/%y %H:%M:%S')

delta_time_obj=datetime_object2 - datetime_object
delta_time_obj_sec=delta_time_obj.total_seconds()

print(datetime_object)
print(datetime_object.date())
print(datetime_object.time())
print(datetime_object.hour)



#row 0 is column name, column 0 is id of entry, so start from 1,1 
#Lines[1].split(',')[1]  

Trips[1].split(',')[1] # pick up time of first entry
Trips[1].split(',')[2] # drop off time of first entry
Trips[1].split(',')[3] # drop off time of first entry
Trips[1].split(',')[4] # drop off time of first entry


#PickDT=Trips[1].split(',')[1]
#DropDT=Trips[1].split(',')[2]
PickDT=datetime.strptime(  Trips[1].split(',')[1] , '%Y-%m-%d %H:%M:%S')
DropDT=datetime.strptime(  Trips[1].split(',')[2] , '%Y-%m-%d %H:%M:%S')
PickLId=Trips[1].split(',')[3]
DropLId=Trips[1].split(',')[4]

#PickDate=datetime.strptime( PickDT, '%Y-%m-%d %H:%M:%S').date()
#PickTime=datetime.strptime( PickDT, '%Y-%m-%d %H:%M:%S').time()
#DropTime=datetime.strptime( DropDT, '%Y-%m-%d %H:%M:%S').time()
TravelTime = (DropDT - PickDT).total_seconds()
PickTimeAbs = PickDT.hour*60*60+PickDT.minute*60+PickDT.second   



def getTrip(cnt):
    PickDT=datetime.strptime(  Trips[cnt].split(',')[1] , '%Y-%m-%d %H:%M:%S')
    DropDT=datetime.strptime(  Trips[cnt].split(',')[2] , '%Y-%m-%d %H:%M:%S')
    PickLId=Trips[cnt].split(',')[3]
    DropLId=Trips[cnt].split(',')[4]
    TravelTime = (DropDT - PickDT).total_seconds()
    PickTimeAbs = PickDT.hour*60*60+PickDT.minute*60+PickDT.second
    return PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs


PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs= getTrip(2)
print(DropDT)



### pre-skip , skip 0-10 hour

PickTimeAbs = 0
BeginTime = 10*3600 # start from 10

while PickTimeAbs < BeginTime :
    PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs = getTrip(count)
    #print(PickDT)
    #print(PickTimeAbs)
    count +=1

print(count)
print(PickTimeAbs)

preCount=count  # total count before setting start

### set up init to redis db 

setUpTime=0.8*3600 + BeginTime # an hour trip as set up

r1=redis.Redis()
r1.flushdb() ## clean db

#cnt161=0
#minLId=999
#maxLId=0

# zone Id from 1 to 265
# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

while PickTimeAbs < setUpTime :
    PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs = getTrip(count)
    r1.zadd(PickLId,{"taxi:{}".format(count-preCount):setUpTime-1})  ## all set to 10:30 arrive to init
    #print(PickDT)
    #print(PickTimeAbs)
 #   if PickLId == '161' :
 #       cnt161 +=1
    count +=1

# r1.zcount('142', float("-inf") , float("inf"))
## extra set up, add 2 taxi to each zone
sudocount=count-preCount

for x in range(maxZoneId+1):
    r1.zadd("{}".format(x),{"taxi:{}".format(sudocount):setUpTime-1})  ## all set to 10:30 arrive to init
    sudocount += 1
    r1.zadd("{}".format(x),{"taxi:{}".format(sudocount):setUpTime-1})  ## all set to 10:30 arrive to init
    print(r1.zcount("{}".format(x),float("-inf") , float("inf")))
    sudocount += 1

#print(cnt161)

#r1.zscan(PickLId,0)  ## print locID all taxi
#rp=r1.zpopmin(PickLId)


## simple exmaple of update 1 driver by request
count +=1
PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs = getTrip(count)
rp=r1.zpopmin(PickLId)
r1.zadd(DropLId,{rp[0][0]:PickTimeAbs+TravelTime}) ## update zone taxi queue
WaitTimeS=PickTimeAbs-rp[0][1]
print(WaitTimeS)

#int(rp[0][0][5:].decode("utf-8")) ## convert zone id from byte to int for storing result



## only record last one
## need to record everything in SQL later
ZoneWaitTime=[-1]*266 # init zone waiting time with size 266 (1-265) 


## update taxi based on request
EndTime=18*3600
#EndTime=42000

## structure should be double loop
## first while time increasement
## second while do what ever should be done whihin these time period 

import sys

currentTime = setUpTime 

start_time = time.time()
# your code
elapsed_time = time.time() - start_time

updateZoneTime=0
getnewTripTime=0
updateRequestTime=0

while currentTime < EndTime:
    ## check all update table
    print("update zone ")

    start_time = time.time()
    for i in range(maxZoneId):
        while True :
            rpf=r1.zpopmin("F{}".format(i+1)) 
            if rpf == [] :  ## no taxi
                break
            if rpf[0][1]>currentTime :  ## taxi hasn't arrived , put it back to future
                r1.zadd("F{}".format(i+1),{rpf[0][0]:rpf[0][1]})
                break
            r1.zadd("{}".format(i+1),{rpf[0][0]:rpf[0][1]}) # put taxi into wating queue 

    updateZoneTime += time.time() - start_time

    start_time = time.time()
    ## get new trips, put into request
    print("get new trips")
    while PickTimeAbs < currentTime:
        PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs = getTrip(count)
        r1.rpush("re_lFromId",PickLId)  ## three list for 
        r1.rpush("re_lToId",DropLId)
        r1.rpush("re_lTime",TravelTime)
        count += 1


    getnewTripTime += time.time() - start_time


    start_time = time.time()

    ## match request with table, update waiting time, send new info to future table
    dummyI=0
    len_l=r1.llen("re_lFromId")
    print("l_length = {}".format(r1.llen("re_lFromId")))
    print("update requests")
    if len_l >600 :
        sys.exit()
    while dummyI < len_l : 
        PickLId = r1.lpop("re_lFromId").decode("utf-8")
        DropLId = r1.lpop("re_lToId").decode("utf-8")
        TravelTime = float(r1.lpop("re_lTime").decode("utf-8"))
       
        rp=r1.zpopmin(PickLId)      ## check taxi match request 
        if rp == [] : ## not matched , send request back to head of queue
            r1.rpush("re_lFromId",PickLId)  
            r1.rpush("re_lToId",DropLId)
            r1.rpush("re_lTime",TravelTime)
        else :  ## matched , update waiting time & future table
            len_l -=1
            WaitTimeS=currentTime-rp[0][1]
            if WaitTimeS < 0 : ## bug here
                print("negative WaitTime")
            ZoneWaitTime[int(PickLId)]=WaitTimeS ## need to migirate to sql later
            r1.zadd("F"+DropLId,{rp[0][0]:currentTime+TravelTime}) ## update zone taxi queue                    ## add taxi with future arriving time
            #print("match , l_length = {}".format(r1.llen("re_lFromId")))

        dummyI += 1

    updateRequestTime += time.time() - start_time


    ## special match , while waiting  queue>100 & waiting time > 3600, re-distribute
    while r1.llen("re_lFromId") > 100:
        PickLId = r1.lpop("re_lFromId").decode("utf-8")
        DropLId = r1.lpop("re_lToId").decode("utf-8")
        TravelTime = float(r1.lpop("re_lTime").decode("utf-8"))
        for i in range(maxZoneId):
            while True :
                rps=r1.zpopmin("{}".format(i+1))
   ## continue here 



    currentTime += 3
    print("currentTime = {}".format(currentTime))

print(updateZoneTime)
print(getnewTripTime)
print(updateRequestTime)

print(ZoneWaitTime)

## PickLId = r1.lpop("re_lFromId").decode("utf-8")
## print(PickLId)
## rp=r1.zpopmin(PickLId)
## print(rp)


sys.exit()

r1.hmset("myhash",{"field":"foo"}) #hmset(name, mapping)
r1.hgetall("myhash") # {b'field': b'foo'}
r1.hget("myhash","field") #b'foo'

## using three list for request trip
# re_lFromId
# re_lToId
# re_lTime

r1.rpush("re_lFromId",1)
r1.rpush("re_lToId",2)
r1.rpush("re_lTime",110.0)

r1.rpush("re_lFromId",1)
r1.rpush("re_lToId",5)
r1.rpush("re_lTime",120.0)

r1.lindex("re_lFromId",1)

r1.llen("re_lToId")


#r1.lpop("re_lFromId")
#r1.rpush("re_lFromId","1")


import sys
sys.exit()


while PickTimeAbs < EndTime:
    PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs = getTrip(count)
    rp=r1.zpopmin(PickLId)
    if rp == [] :
        print("zone empty ,  break")
        break
    r1.zadd(DropLId,{rp[0][0]:PickTimeAbs+TravelTime}) ## update zone taxi queue
    WaitTimeS=PickTimeAbs-rp[0][1]
    if WaitTimeS < 0 :
        print("negative WaitTime")
        break
    ZoneWaitTime[int(PickLId)]=WaitTimeS
    count +=1


## >>> rp[0][0] ## taxi id
## b'taxi:1043'
## >>> rp[0][1] ## last arrive time
## 37800.0

## empty case : remp=r1.zpopmin('31')
## []



### update based on incoming trip


## > < == op are supported with datetime and datetime.time 


## for readline format , there is additional '\n ' in last element, could be extracted with Trips[1].split(',')[5].split('\n')[0]



#for line in Lines: 
#    print(line.strip())




# import csv
# with open('0604_clean_request_test.csv', newline='') as csvfile :
#     spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
#     for row in spamreader:
#         print (', '.join(row))
    
    


