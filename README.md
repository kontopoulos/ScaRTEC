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

# Run a demo

First, we need to run the RDFizer to produce results for the event recognition engine:

```
cd /home/ikontopoulos/st_rdf_minimal_example/

gradle -PmainClassName=ST_RDF_RDFizer -Pmyargs=/home/aglenis/gitlab_repos/RDFizer-templates/AIS_SocketConnector/Template.q, execute
```

Then, we need to run the event recognition engine. Open another terminal and type:

```
cd /home/ikontopoulos/

scala -J-Xmx4g ScaRTEC.jar 3600 3600
```

The first parameter is the sliding step and the second parameter is the window size, both in seconds. While the ER engine is running you should be able to see something like this:

```
ER: (1443650401 - 1443654001]
ER: (1443654001 - 1443657601]
ER: (1443657601 - 1443661201]

```

This is the segment of the stream in which the ER is happening.
