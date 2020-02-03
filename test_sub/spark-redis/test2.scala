import com.redislabs.provider.redis._


// connect to redis server

val redisServerDnsAddress =  "ip-10-0-0-9.us-west-2.compute.internal"  //"REDIS_HOSTNAME"
val redisPortNumber = 6379
val redisPassword = ""
val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber))
//val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))

// set write to redis
val stringSetRDD = sc.parallelize(Seq("member1", "member2", "member3", "member3"))
sc.toRedisSET(stringSetRDD, "setkey1")(redisConfig)


val keysRDD = sc.fromRedisKeyPattern("setkey*")(redisConfig)
val setRDD = keysRDD.getSet
setRDD.collect()


// sorted set
//sc.toRedisZSET(zsetRDD, zsetName)

//val stringZSetRDD = Seq("m1","m2","m3")
//val stringZSetRDD2 = Seq("1","2","3")
val stringZSetRDD = sc.parallelize(Seq(("field1", "11"), ("field2", "22")))
sc.toRedisZSET(stringZSetRDD, "zsetkey1")(redisConfig)
val keysRDD=sc.fromRedisKeyPattern("zsetkey*")(redisConfig)
val zsetRDD=keysRDD.getZSet

// keys RDD , data access in Redis is based on keys, first  need a keys RDD.

val keysRDD = sc.fromRedisKeyPattern("foo*", 5)
val keysRDD = sc.fromRedisKeys(Array("foo", "bar"), 5)

val zsetRDD = sc.fromRedisZSetWithScore("keyPattern*")
val zsetRDD = sc.fromRedisZSetWithScore(Array("foo", "bar"))


// 
