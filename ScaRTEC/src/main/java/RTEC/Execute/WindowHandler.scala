package RTEC.Execute

import RTEC.Data
import RTEC.Data.ExtraLogicReasoning

/**
  * Created by ikon on 30-Nov-16.
  */
object WindowHandler {

  private var inputDir = ""
  private var outputFile = ""
  // number of windows
  private var numWindows = 0
  private var totalRecognitionTime = 0L
  // number of low level events
  private var SDEs = 0
  // number of complex events produced
  private var CEs = 0
  // initiations of non terminated simple fluents
  private var _pendingInitiations: Iterable[((Data.FluentId, Seq[String]), Set[Int])] = Iterable()
  // non terminated statically determined fluents
  private var _nonTerminatedSDFluents: Map[Data.FluentId, Map[Seq[String], Data.Intervals]] = Map()
  // entities (e.g., vessel ids from dynamic grounding) from previous window that need to be transferred to the next
  private var _previousWindowEntities: Map[String, Iterable[Seq[String]]] = Map()
  // variables below are for the spatial preprocessing
  // projections based on trajectories
  private var projections: Map[String,Map[Int,(Seq[String], Seq[String])]] = Map()
  // proximity of vessels
  private var proximity: Map[(String,String),Seq[(Int,Int)]] = Map()

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
    * @param startTime
    * @param clock
    */
  def performER(windowSize: Int, slidingStep: Int, lastTime: Int, startTime: Int, clock: Int): Unit = {
    var batchLimit = startTime
    // Parse declarations and patterns (definitions)
    Reader.Main.readDeclarations(s"$inputDir/declarations.txt")
    Reader.Main.readDefinitions(s"$inputDir/definitions.txt")

    /*================= Maritime Domain ==================*/
    // ScaRTEC is a general event recognition engine
    // Below ScaRTEC reads additional data for the maritime domain
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

    // get the file of the stream
    val stream = scala.io.Source.fromFile(s"$inputDir/dataset.txt").getLines
    // holds the data of the batch
    var batch = Vector.empty[String]
    var batch_tmp = Vector.empty[String]

    // start event recognition to the stream
    scala.util.control.Breaks.breakable {
      stream.foreach {
        str =>
          if (getEventTime(str) <= batchLimit) {
            // until upper limit is reached store to batch
            batch :+= str
          }
          else {
            numWindows += 1
            // if last time is reached break the loop
            if (batchLimit >= lastTime) scala.util.control.Breaks.break
            val lowerLimit = batchLimit - windowSize
            // drop any leftovers from previous window
            batch = batch.dropWhile(getEventTime(_) <= lowerLimit)
            if (getEventTime(batch.head) > batchLimit) batch_tmp = Vector() else batch_tmp = batch
            println(s"ER: ($lowerLimit - $batchLimit]")

            //batchSpatialPreprocessing(batch_tmp,batchLimit,lowerLimit, lastTime)
            batchEventRecognition(batch_tmp, batchLimit, lowerLimit, clock, staticData,numWindows)

            // add event to batch
            batch :+= str
            // increase the batch limit (move to the next window)
            batchLimit += slidingStep
          }
      }
    }
    // end of stream

    // finally perform event recognition on the last batch
    // numWindows += 1
    val lowerLimit = batchLimit-windowSize
    // drop any leftovers from previous window
    batch = batch.dropWhile(getEventTime(_) <= lowerLimit)
    println(s"ER: ($lowerLimit - $batchLimit]")

    //batchSpatialPreprocessing(batch,batchLimit,lowerLimit,lastTime)
    batchEventRecognition(batch,batchLimit,lowerLimit,clock,staticData,numWindows)
    // end of event recognition


    /* ==== some statistics ====*/
    // avg number of low level events
    val avgSDEs = SDEs/numWindows
    // avg number of complex events
    val avgCEs = CEs/numWindows
    var denominator = 1
    if ((numWindows-1) > 0) denominator = numWindows-1
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
  private def batchEventRecognition(batch: Vector[String], batchLimit: Int, lowerLimit: Int, clock: Int, staticData: ((Set[Data.InstantEvent], Set[Data.Fluent], (Seq[Data.IEPredicate], Seq[Data.FPredicate]), (Set[Data.InputEntity], Seq[Data.BuiltEntity]), Seq[(Data.EventId, String)])), currentWindow: Int): Unit = {
    // parse the input batch of events
    val input = parseStreamBatch(batch)

    val windowReasoner = new Reasoner
    // update all information needed for current window
    windowReasoner.updateNonTerminatedInitiations(_pendingInitiations)
    windowReasoner.updateSDFluents(_nonTerminatedSDFluents)
    windowReasoner.updateCurrentWindowEntities(_previousWindowEntities)

    // start event recognition
    val recTime = windowReasoner.run(staticData, input, outputFile, lowerLimit, batchLimit, clock)
    if (currentWindow != 1) totalRecognitionTime += recTime._1
    CEs += windowReasoner.numComplexEvents

    // get all information needed from previous window
    _pendingInitiations = windowReasoner.getNonTerminatedSimpleFluents
    _nonTerminatedSDFluents = windowReasoner.getNonTerminatedSDFluents
    _previousWindowEntities = windowReasoner.getPreviousWindowEntities
  }

  private def batchSpatialPreprocessing(batch: Vector[String], batchLimit: Int, lowerLimit: Int, lastTime: Int): Unit =
  {
    // parse the input batch of events
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

}
