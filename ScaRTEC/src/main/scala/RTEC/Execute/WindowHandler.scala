package RTEC.Execute

import java.util.{Collections, Properties}

import scala.collection.JavaConverters._
import RTEC.Data
import RTEC.Data.ExtraLogicReasoning
import integrated._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by ikon on 30-Nov-16.
  */
object WindowHandler {

  private var inputDir = ""
  private var outputFile = ""
  private var numWindows = 0L
  private var totalRecognitionTime = 0L
  private var SDEs = 0L
  private var CEs = 0L
  private var _pendingInitiations: Iterable[((Data.FluentId, Seq[String]), Set[Long])] = Iterable()
  private var _nonTerminatedSDFluents: Map[Data.FluentId, Map[Seq[String], Data.Intervals]] = Map()
  private var _previousWindowEntities: Map[String, Iterable[Seq[String]]] = Map()
  private var projections: Map[String,Map[Long,(Seq[String], Seq[String])]] = Map()
  private var proximity: Map[(String,String),Seq[(Long,Long)]] = Map()

  /**
    * Sets the parameters below
    * @param in input directory
    * @param out output file
    */
  def setIOParameters(in: String, out: String): Unit = {
    inputDir = in
    outputFile = out
  }

  /**
    * Performs event recognition to the stream
    * @param windowSize
    * @param slidingStep
    * @param clock
    */
  def performER(windowSize: Long, slidingStep: Long, clock: Long): Unit = {
    var batchLimit = 0L
    // Read input files
    Reader.Main.readDeclarations(s"$inputDir/declarations.txt")
    Reader.Main.readDefinitions(s"$inputDir/definitions.txt")

    /*================= Maritime Domain ==================*/
    try {
      ExtraLogicReasoning.readPorts(s"$inputDir/static_data/ports.csv")
      ExtraLogicReasoning.readGrid(s"$inputDir/static_data/grid.csv")
      ExtraLogicReasoning.readPortsPerCell(s"$inputDir/static_data/ports_per_cell.csv")
      ExtraLogicReasoning.readRelevantAreas(s"$inputDir/static_data/all_areas/areas.csv")
      ExtraLogicReasoning.readPolygons(s"$inputDir/static_data/all_areas/polygons.csv")
      ExtraLogicReasoning.readSpeedLimits(s"$inputDir/static_data/all_areas/areas_speed_limits.csv")
    }
    catch {
      case e: Exception => println(s"Warning (maritime domain): ${e.getMessage}")
    }
    /*================= Maritime Domain ==================*/


    // Parse static data to the internal structures
    val staticData = Reader.Main.staticData match {
      case Some(sd) => sd
      case _ => println("Error while reading static data"); null
    }

    // holds the data of the batch
    var batch = Vector.empty[String]
    var batch_tmp = Vector.empty[String]

    val TOPIC="sample_rdfizer_topic2"

    val  props = new Properties()
    props.put("bootstrap.servers", "192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", classOf[ST_RDF_KryoSerializer].getName)
    props.put("group.id", "rdfizer_group")

    val consumer = new KafkaConsumer[String, ST_RDF](props)

    consumer.subscribe(Collections.singletonList(TOPIC))

    while (true) {
      val records = consumer.poll(10000)
      for (record <- records.asScala) {
        val annotated = convertRDFPart(record.value)
        //println(annotated)
        val currentEventTime = record.value.time1/1000
        if (batchLimit == 0L) batchLimit = currentEventTime + slidingStep
        if (currentEventTime <= batchLimit) {
          // until upper limit is reached store to batch
          batch ++= annotated
          //println(batch)
        }
        else {
          numWindows += 1L
          val lowerLimit = batchLimit - windowSize
          // drop any leftovers from previous window
          batch = batch.dropWhile(getEventTime(_) <= lowerLimit)
          //if (getEventTime(batch.head) > batchLimit) batch_tmp = Vector() else batch_tmp = batch
          println(s"ER: ($lowerLimit - $batchLimit]")

          //batchSpatialPreprocessing(batch_tmp,batchLimit,lowerLimit, lastTime)
          batchEventRecognition(batch, batchLimit, lowerLimit, clock, staticData, numWindows)

          batch ++= annotated
          batchLimit += slidingStep
        }
      }
    }
    // end of stream
    consumer.close

    // finally perform event recognition on the last batch
    // numWindows += 1
    val lowerLimit = batchLimit-windowSize
    // drop any leftovers from previous window
    batch = batch.dropWhile(getEventTime(_) <= lowerLimit)
    println(s"ER: ($lowerLimit - $batchLimit]")

    //batchSpatialPreprocessing(batch,batchLimit,lowerLimit,lastTime)
    batchEventRecognition(batch,batchLimit,lowerLimit,clock,staticData,numWindows)
    // end of event recognition


    val avgSDEs = SDEs/numWindows
    val avgCEs = CEs/numWindows
    var denominator = 1L
    if ((numWindows-1L) > 0L) denominator = numWindows-1L
    //val denominator = numWindows
    val avgRecTime = totalRecognitionTime.toDouble/denominator
    println(avgRecTime)
    // write recognition statistics to file
    val fd = new java.io.FileWriter("statistics.txt",true)
    fd.write(s"Window Size: $windowSize -> Avg Rec Time: $avgRecTime ms | Avg SDEs: $avgSDEs | Avg CEs: $avgCEs \n")
    fd.close()
  }


  /**
    * Performs event recognition for current batch
    * @param batch current batch data
    * @param batchLimit
    * @param lowerLimit
    * @param clock
    * @param staticData
    */
  private def batchEventRecognition(batch: Vector[String], batchLimit: Long, lowerLimit: Long, clock: Long, staticData: ((Set[Data.InstantEvent], Set[Data.Fluent], (Seq[Data.IEPredicate], Seq[Data.FPredicate]), (Set[Data.InputEntity], Seq[Data.BuiltEntity]), Seq[(Data.EventId, String)])), currentWindow: Long): Unit = {
    val input = parseStreamBatch(batch)

    val windowReasoner = new Reasoner
    // update all information needed for current window
    windowReasoner.updateNonTerminatedInitiations(_pendingInitiations)
    windowReasoner.updateSDFluents(_nonTerminatedSDFluents)
    windowReasoner.updateCurrentWindowEntities(_previousWindowEntities)

    // Start event recognition
    val recResults = windowReasoner.run(staticData, input, outputFile, lowerLimit, batchLimit, clock)
    if (currentWindow != 1L) totalRecognitionTime += recResults._1

    // send recognition results to kafka topic
    sendToKafka(recResults._3)
    /*val fd = new java.io.FileWriter(outputFile, true)
    fd.write(recResults._3)
    fd.close*/


    CEs += windowReasoner.numComplexEvents

    // get all information needed from previous window
    _pendingInitiations = windowReasoner.getNonTerminatedSimpleFluents
    _nonTerminatedSDFluents = windowReasoner.getNonTerminatedSDFluents
    _previousWindowEntities = windowReasoner.getPreviousWindowEntities
  }

  private def batchSpatialPreprocessing(batch: Vector[String], batchLimit: Long, lowerLimit: Long, lastTime: Long): Unit =
  {
    val input = parseStreamBatch(batch)

    val windowReasoner = new SpatialReasoner
    // update all information needed for current window
    windowReasoner.updateTrajectories(projections)
    windowReasoner.updateProximity(proximity)

    val recTime = windowReasoner.run(input, outputFile, lowerLimit, batchLimit, lastTime)
    totalRecognitionTime += recTime

    // get all information needed from previous window
    projections = windowReasoner.getProjections
    proximity = windowReasoner.getProximity
  }

  /**
    * Gets the time of current event
    * @param stream current Event
    * @return time
    */
  private def getEventTime(stream: String): Long = {
    val parts = stream.split("[()]")
    if (parts.length > 1) {
      parts.last.split("-").last.toLong
    }
    else {
      stream.split(" ").last.toLong
    }
  }

  /**
    * Parses batch data
    * @param batch input
    * @return
    */
  private def parseStreamBatch(batch: Vector[String]) = {
    SDEs += batch.size
    // parse real time input
    Reader.ParseRealTimeInput.get(batch.mkString) match {
      case Some(d) => d
      case _ => println("Error while reading dataset"); null
    }
  }

  private def convertRDFPart(input: ST_RDF): Vector[String] = {

    val annotation = Map("stoppedinit" -> "stop_start",
      "stoppedend" -> "stop_end",
      "slowmotionstart" -> "slow_motion_start",
      "slowmotionend" -> "slow_motion_end",
      "gapend" -> "gap_end",
      "headingchange" -> "change_in_heading",
      "speedchangestart" -> "change_in_speed_start",
      "speedchangeend" -> "change_in_speed_end")

    val parts = input.rdfPart.replaceAll("[;\\s]","").split("[:]")
    // name of event
    val name = annotation(parts(1).toLowerCase)
    // vessel id
    val id = parts.filter(_.contains("ves")).head
    // heading of vessel
    val heading = parts.filter(_.contains("hasHeading")).head.replace("hasHeading\"","").replace("\"^^unit","")
    // speed of vessel
    val speed = new java.math.BigDecimal(parts.filter(_.contains("hasSpeed")).head.replace("hasSpeed\"","").replace("\"^^unit","").toDouble*3.6).toPlainString.take(12)
    val ts = input.time1/1000
    val lon = input.longitude
    val lat = input.latitude

    Vector(s"HappensAt [$name $id] $ts",s"HappensAt [coord $id $lon $lat] $ts",s"HappensAt [velocity $id $speed $heading] $ts")
  }

  def sendToKafka(output: String): Unit = {
    val  props = new Properties()
    props.put("bootstrap.servers", "192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC="sample_cer_topic"

    val record = new ProducerRecord(TOPIC, "localhost", output)
    producer.send(record)

    producer.close
  }

}
