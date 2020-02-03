import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.{Row, SparkSession}

sc.stop() // loading spark with tar precreate a sc..

val sc1 = new SparkContext(new SparkConf()
      .setMaster("local")
      .setAppName("myApp")
      // initial redis host - can be any node in cluster mode
      .set("spark.redis.host", "localhost")
      // initial redis port
      .set("spark.redis.port", "6379")
      // optional redis AUTH password
      //.set("spark.redis.auth", "passwd")
  )

//val spark = SparkSession
//  .builder()
//  .appName("myApp")
//  .master("local[*]")
//  .config("spark.redis.host", "localhost")
//  .config("spark.redis.port", "6379")
//  .getOrCreate()

//val keysRDD = sc.fromRedisKeyPattern("foo*", 5)
//val keysRDD = sc.fromRedisKeys(Array("foo", "bar"), 5)

val zsetRDD = sc1.fromRedisZSetWithScore("keyPattern*")
val zsetRDD = sc1.fromRedisZSetWithScore(Array("foo", "bar"))

