## python spark-kafka  package : --packages org.apache.spark:spark-streaming-kafka-0-8_2.12:2.4.4 
## deploying with packages ./bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.12:2.4.4 ...

## using import to load packages

## https://spark.apache.org/docs/latest/streaming-kafka-integration.html

## run it spark-submit hello_spark_kafka_stream.py 

#import os   ## import inside python, not work ..
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark-shell'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import redis

#import json

sc = SparkContext(appName="SparkStream")
# sc.setLogLevl("Error") ## no this attribute
packTime = 2 ## streaming batch interval : 2 secs
ssc = StreamingContext(sc,packTime)  ##

topic='TaxiData'
#kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list":['10.0.0.11:9092','10.0.0.13:9092','10.0.0.14:9092']})
kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list":'10.0.0.11:9092'})

lines = kafkaStream.map(lambda x: x[1])


## print("\n\nhello \n\n")
## kafkaStream.pprint()
## lines.count().pprint()
print( type(lines.pprint()))
## print(lines)
## print(lines.count())
## print(lines.count().pprint())
## 
## print("\n\n\hello2 \n\n")

ssc.start()

ssc.awaitTermination()




#kafkaStream = KafkaUtils.createStream(streamingContext, \
#     [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])

# directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

