## python spark-kafka  package : --packages org.apache.spark:spark-streaming-kafka-0-8_2.12:2.4.4 

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.12:2.4.4 pyspark-shell'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#import json
sc = SparkContext(appName="SparkStream")
sc.setLogLevl("Error")
ssc = StreamingContext(sc,2)


