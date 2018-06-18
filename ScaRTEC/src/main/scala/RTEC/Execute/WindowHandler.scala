package RTEC.Execute

import java.util.{Collections, Properties, UUID}

import scala.collection.JavaConverters._
import RTEC.Data
import RTEC.Data.ExtraLogicReasoning
import integrated._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

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
  val annotation = Map("StoppedInit" -> "stop_start",
    "StoppedEnd" -> "stop_end",
    "SlowMotionStart" -> "slow_motion_start",
    "SlowMotionEnd" -> "slow_motion_end",
    "GapEnd" -> "gap_end",
    "HeadingChange" -> "change_in_heading",
    "SpeedChangeStart" -> "change_in_speed_start",
    "SpeedChangeEnd" -> "change_in_speed_end")

  // key -> (event name,arity), value -> (arguments,earlier_timestamp)
  private var eventTimestamps: Map[(String,Int), Map[Seq[String],Long]] = Map()

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
      //ExtraLogicReasoning.readRelevantAreas(s"$inputDir/static_data/all_areas/areas.csv")
      //ExtraLogicReasoning.readPolygons(s"$inputDir/static_data/all_areas/polygons.csv")
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

    //val TOPIC="sample_rdfizer_topic2"
    val TOPIC="ais_near_ais_final_first"

    val props = new Properties()
    props.put("bootstrap.servers", "192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", classOf[ST_RDF_KryoSerializer].getName)
    props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,classOf[ST_RDFConsumerInterceptor].getName)
    //props.put("group.id", "rdfizer_group")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "rdfizer_group")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, ST_RDF](props)

    consumer.subscribe(Collections.singletonList(TOPIC))

    while (true) {
      //println("I entered")
      val records = consumer.poll(10000)
      for (record <- records.asScala) {
        if (validEvent(record.value.rdfPart)) {
          //println(record.value.rdfPart)
          val annotated = convertRDFPart(record.value) // (events,ts,ingestionTimestamp)




          // key -> (event name,arity), value -> (arguments,timestamp)
          //private var eventTimestamps: Map[(String,Int), Map[Seq[String],Set[Long]]] = Map()

          //println(annotated)
          //val currentEventTime = record.value.time1 / 1000
          val currentEventTime = annotated._2
          if (batchLimit == 0L) batchLimit = currentEventTime + slidingStep
          if (currentEventTime <= batchLimit) {
            // until upper limit is reached store to batch
            batch ++= annotated._1

            storeIngestionTimestamps(annotated)
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

            eventTimestamps = Map() // empty map for new window ingestion timestamps
            storeIngestionTimestamps(annotated)
            batch ++= annotated._1
            batchLimit += slidingStep
          }
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

  private def storeIngestionTimestamps(annotated: (Vector[String],Long,Long)) = {
    val partsOfEvent = annotated._1.head.split(" ")
    val name = partsOfEvent(1).drop(1).replaceAll("\\[","")
    val arguments = partsOfEvent.drop(2).dropRight(1).map(_.replaceAll("\\]","")).toSeq
    val arity = arguments.size

    // store the ingestion timestamps, keep only the earliest chronologically
    eventTimestamps.get(name,arity) match {
      case Some(args) => {
        args.get(arguments) match {
          case Some(t) =>
          case None => {
            var updated = args
            updated += (arguments -> annotated._3)
            eventTimestamps += ((name,arity) -> updated)
          }
        }
      }
      case None => {
        eventTimestamps += ((name,arity) -> Map(arguments -> annotated._3))
      }
    }
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
    val recResults = windowReasoner.run(staticData, input, outputFile, lowerLimit, batchLimit, clock, eventTimestamps)
    if (currentWindow != 1L) totalRecognitionTime += recResults._1

    // send recognition results to kafka topic
    //sendToKafka(recResults._3)
    val fd = new java.io.FileWriter(outputFile, true)
    fd.write(recResults._3)
    fd.close
    println(recResults._3)


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
    //println(batch.mkString("\n"))
    SDEs += batch.size
    // parse real time input
    Reader.ParseRealTimeInput.get(batch.mkString) match {
      case Some(d) => d
      case _ => println("Error while reading dataset"); null
    }
  }

  /*private def convertRDFPart(input: ST_RDF): Vector[String] = {

    println(input.longitude)
    println(input.latitude)
    println(input.time1)
    println(input.rdfPart)

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
  }*/

  private def validEvent(input: String): Boolean = {
    if (input.contains("within") || input.contains("nearTo")) {
      true
    }
    else if (input.replaceAll(" ","").split("[:]").filter(annotation.contains(_)).length > 0) {
      true
    }
    else false
  }

  private def convertRDFPart(input: ST_RDF): (Vector[String],Long,Long) = {

    if (input.rdfPart.contains("within")) {
      val parts = input.rdfPart.split(" ").filterNot(_ == ".").takeRight(3)
      val vesselInfo = parts.head.split("[#]").last.split("[_]")
      val areaId = "areaId_" + parts.last.split("[#]").last.split("[\\\\]").head.dropRight(1)

      val id = vesselInfo(1)
      val ts = vesselInfo(2).toLong/1000
      val lon = vesselInfo(3).toDouble
      val lat = vesselInfo(4).split("[\\\\]").head.dropRight(1).toDouble

      (Vector(s"HappensAt [within $id $areaId] $ts",s"HappensAt [coord $id $lon $lat] $ts"),ts,input.getIngestionTimestamp)
    }
    else if (input.rdfPart.contains("nearTo")) {
      val parts = input.rdfPart.split(" ").filterNot(_ == ".").takeRight(3)

      val vessel1 = parts.head.split("[#]").last.split("[_]")
      val id1 = vessel1(1)
      val ts1 = vessel1(2).toLong/1000
      val lon1 = vessel1(3).toDouble
      val lat1= vessel1(4).split("[\\\\]").head.dropRight(1).toDouble

      val vessel2 = parts.last.split("[#]").last.split("[_]")
      val id2 = vessel2(1)
      val ts2 = vessel2(2).toLong/1000
      val lon2 = vessel2(3).toDouble
      val lat2 = vessel2(4).split("[\\\\]").head.dropRight(1).toDouble

      (Vector(s"HappensAt [near $id1 $id2] $ts1",s"HappensAt [coord $id1 $lon1 $lat1] $ts1",s"HappensAt [coord $id2 $lon2 $lat2] $ts2"),ts1,input.getIngestionTimestamp)
    }
    else {
      val parts = input.rdfPart.replaceAll("[;]","").split(":").filterNot(s => s.isEmpty || s == ".")
      val events = input.rdfPart.replaceAll(" ","").split("[:]").filter(annotation.contains(_))

      val vesselInfo = parts.filter(_.contains("node_")).head.split("[_]")
      val info = parts.filter(x => x.contains("hasSpeed") || x.contains("hasHeading")).take(2)

      val id = vesselInfo(1)
      val ts = vesselInfo(2).toLong/1000
      val lon = vesselInfo(3).toDouble
      val lat = vesselInfo(4).replace(" .","").dropRight(1).toDouble
      val speed = info.last.replace("hasSpeed ","").replace("^^unit","").replace("\"","").toDouble*3.6
      val heading = info.head.replace("hasHeading ","").replace("^^unit","").replace("\"","").toDouble

      (events.map{
        x =>
          val name = annotation(x)
          s"HappensAt [$name $id] $ts"
      }.toVector ++ Vector(s"HappensAt [coord $id $lon $lat] $ts",s"HappensAt [velocity $id $speed $heading] $ts"),ts,input.getIngestionTimestamp)
    }
  }

}
