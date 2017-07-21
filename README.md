# ScaRTEC
Complex event recognition engine based on the Event Calculus formalism

Implemented in Scala 2.11.7

Contains a sample dataset of maritime data (first week of october 2015)

# Kafka topic on the cluster

The Kafka topic for the event recognition results has been created in the following way:

```
cd /home/aglenis/confluent-3.1.1/

./bin/kafka-topics --create --zookeeper 192.168.1.2:2181, 192.168.1.3:2181,192.168.1.5:2181 --replication-factor 2 --partitions 6 --topic sample_cer_topic
```


