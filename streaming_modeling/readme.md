checking kafka topic and message by using kafka drop:

https://github.com/obsidiandynamics/kafdrop

sudo docker run -d --rm -p 9000:9000 -e KAFKA_BROKERCONNECT=10.0.0.11:9092,10.0.0.13:9092,10.0.0.14:9092    -e JVM_OPTS="-Xms32M -Xmx64M"     -e SERVER_SERVLET_CONTEXTPATH="/"     obsidiandynamics/kafdrop



## clean message in topic
kafka-topics --zookeeper localhost:2181 --delete --topic TaxiData

