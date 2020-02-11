import redis

#r3= redis.Redis(  host='localhost', port=6379 )
r3= redis.Redis(  host='10.0.0.12', port=6379 )

r3.zadd("Zone1Arr",{"taxi:01":151})
r3.zadd("Zone1Arr",{"taxi:02":154})
r3.zadd("Zone1Arr",{"taxi:03":138})

print(r3.zscan("Zone1Arr",0))
r3p=r3.zpopmin("Zone1Arr") ## r3p is a list with only one entry

if r3.zpopmin("Zone1Arr") == [] :
  print("empty")




