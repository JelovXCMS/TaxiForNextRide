## input : cleaned csv file of one day yellow taxi (readin as line by line)
## use redis as database
## 

import redis


infile=open("0604_clean_request_test.csv", "r")
Trips = infile.readlines() 
nTrip = len(Trips)
count=1;


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

setUpTime=0.5*3600 + BeginTime # half hour trip as set up

r1=redis.Redis()
r1.flushdb() ## clean db

cnt161=0

while PickTimeAbs < setUpTime :
    PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs = getTrip(count)
    r1.zadd(PickLId,{"taxi:{}".format(count-preCount):setUpTime})  ## all set to 10:30 arrive to init
    #print(PickDT)
    #print(PickTimeAbs)
    if PickLId == '142' :
        cnt161 +=1
    count +=1


print(cnt161)

#r1.zscan(PickLId,0)  ## print locID all taxi
#rp=r1.zpopmin(PickLId)


## simple exmaple of update 1 driver by request
count +=1
PickDT,DropDT,PickLId,DropLId,TravelTime,PickTimeAbs = getTrip(count)
rp=r1.zpopmin(PickDT)
r1.zadd(DropLId,{rp[0][0]:PickTimeAbs+TravelTime})

## >>> rp[0][0]
## b'taxi:1043'
## >>> rp[0][1]
## 37800.0

## empty case : remp=r1.zscan('31')
## (0, [])
## remp[1]
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
    
    


