package RTEC.Execute

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Application extends App {

  override def main(args: Array[String]): Unit = {

    kafkaSend()
    //kafkaReceive()
    execute()

  }

  def kafkaSend(): Unit = {
    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC="Hello-Kafka"

    scala.io.Source.fromFile("./maritime/dataset.txt").getLines.foreach{
      line =>
        //Thread.sleep(100)
        val record = new ProducerRecord(TOPIC, "localhost", line)
        producer.send(record)
    }

    producer.close
  }

  private def execute(): Unit = {

    val inputDir = "./maritime/"
    val outputFile = "./spatial_8.txt"
    // step
    val slidingStep = 3600
    // working memory
    val windowSize = 3600
    // start recognition from time point below
    //val startTime = 1443661203 + slidingStep
    val startTime = 1443661203 + slidingStep
    // last time
    val lastTime = 1443664804
    //val lastTime = 1454284813
    // dataset specific clock
    val clock = 1

    // set input directory and output file
    WindowHandler.setIOParameters(inputDir,outputFile)
    // start event recognition loop
    WindowHandler.performER(windowSize,slidingStep,lastTime,startTime,clock)

  }

  /*def kafkaReceive(): Unit = {
    val TOPIC="Hello-Kafka"

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "group1")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(Collections.singletonList(TOPIC))

    while(true){
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        println(record.value())
      }
    }
  }*/

}

