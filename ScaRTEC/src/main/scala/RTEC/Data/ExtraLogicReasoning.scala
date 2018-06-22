package RTEC.Data

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, LineString, Polygon}
import com.vividsolutions.jts.operation.valid.IsValidOp

import scala.collection.mutable.ListBuffer

/**
  * @author Kontopoulos Ioannis
  */
object ExtraLogicReasoning {

  // variable that stores coordinates per port
  private var ports: Map[String,(String,String)] = Map()
  // variable that stores grid cells per coordinates
  private var gridCells: Map[(Double,Double),String] = Map()
  // variable that stores coordinates per grid cell
  private var cells: Map[String,(Double,Double)] = Map()
  // variable that stores ports per cell
  private var portsInCells: Map[String,Set[String]] = Map()
  // variable that stores relevant areas per grid cell
  private var relevantAreas: Map[(Double,Double),Array[(String,String,String,String,String)]] = Map()
  // variable that stores edges of polygon per area
  private var polygonEdges: Map[String,Array[(Double,Double)]] = Map()
  // variable that stores speed limits per area
  private var speedLimitsAreas: Map[String, Double] = Map()
  // variable that stores fishing vessels
  //private var fishingVessels: Set[String] = Set()
  // variable that stores for each vessel mmsi its type
  private var vesselTypes: Map[String,String] = Map()
  // variable that stores for each vessel type its speed values (min,max,average)
  private var speedTypes: Map[String,(Double,Double,Double)] = Map()
  // variable that stores flight levels per moel
  private var flightLevels: Map[String, (Double,Double)] = Map()
  // variable that stores temporarily information for top of climbs and descents in the current window
  private var topOfCD: Iterable[Predicate.GroundingDict] = Iterable()
  private var _topOfClimb: Map[String, Double] = Map()
  private var _topOfDescent: Map[String, Double] = Map()
  // variable that stores flightplan trajectories
  private var fp_traj: Map[String,LineString] = Map()


  /**
    * Read list of fishing vessels from csv file
    *
    * @param file to read
    */
  /*def readFishingVessel(file: String): Unit = {
    var fishingtoread = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        val mmsiwanted = parts.last
        (mmsiwanted)
    }.toSet
    fishingVessels = fishingtoread
  }*/

  /**
    * Reads list of vessel types from csv file
    * @param file to read
    */
  def readVesselTypes(file: String): Unit = {
    vesselTypes = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        (parts(1),parts(2))
    }.toMap
  }

  /**
    * Reads speed values per vessel type
    * @param file to read
    */
  def readSpeedsPerType(file: String): Unit = {
    speedTypes = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        (parts.head,(parts(1).toDouble,parts(2).toDouble,parts.last.toDouble))
    }.toMap
  }

  /**
    * Reads ports from csv file
    *
    * @param file to read
    */
  def readPorts(file: String): Unit = {
    val portsToRead = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        (parts.head,(parts(1),parts.last))
    }.toArray
      .groupBy(_._1)
      .mapValues(_.map(s => s._2))
      .mapValues(_.head)

    ports = portsToRead
  }

  /**
    * Reads grid from csv file
    *
    * @param file to read
    */
  def readGrid(file: String): Unit = {
    val grid = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        (parts.head,(parts(1),parts.last))
    }.toArray
      .groupBy(_._2)
      .mapValues(_.map(s => s._1))
      .mapValues(_.head)
      .map{case (key,value) => ((key._1.toDouble,key._2.toDouble),value)}

    cells = grid.map(_.swap)
    gridCells = grid
  }

  /**
    * Reads ports per cell from csv file
    *
    * @param file to read
    */
  def readPortsPerCell(file: String): Unit = {
    val pc = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        (parts.head,parts.last)
    }.toArray.groupBy(_._1).mapValues(_.map(_._2).toSet)

    portsInCells = pc
  }

  /**
    * Read relevant areas per grid cell from csv file
    *
    * @param file to read
    */
  def readRelevantAreas(file: String): Unit = {
    val areas = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        ((parts.head.toDouble,parts(1).toDouble),(parts(2),parts(3),parts(4),parts(5),parts.last))
    }.toArray.groupBy(_._1).mapValues(_.map(_._2))

    relevantAreas = areas
  }

  /**
    * Read polygon edges per area from csv file
    *
    * @param file to read
    */
  def readPolygons(file: String): Unit = {
    val polygons = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        val areaId = parts.head
        val edges = parts.drop(1).map{
          e =>
            val pointsOfEdge = e.split("[,]")
            (pointsOfEdge.head.toDouble,pointsOfEdge.last.toDouble)
        }
        (areaId,edges)
    }.toMap

    polygonEdges = polygons
  }

  /**
    * Read speed limits per area from csv file
    *
    * @param file to read
    */
  def readSpeedLimits(file: String): Unit = {
    val limits = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        val areaId = parts.head
        val sl = parts.last.toDouble
        (areaId,sl)
    }.toMap

    speedLimitsAreas = limits
  }

  def getSpeedArea(areaName:String): Double = speedLimitsAreas(areaName)

  /**
    * Gets cell id based on the coordinates
    *
    * @param lon X coordinate
    * @param lat Y coordinate
    * @return
    */
  def getCellId(lon: Double, lat: Double): String = {
    gridCells(lon,lat)
  }

  /**
    * Checks if current vessel is inside a grid cell
    *
    * @param lon
    * @param lat
    * @return
    */
  def vesselInGrid(lon: Double, lat: Double): Boolean = {
    val enclosingCell = SpatialReasoning.getEnclosingGridCell(lon,lat)
    gridCells.contains(enclosingCell._1,enclosingCell._2)
  }

  /**
    * Gets all the cell ids
    *
    * @return cell ids
    */
  def getCellIds: Set[String] = {
    gridCells.map(_._2).toSet
  }

  /**
    * Gets nearby grid cells based on current cell id
    *
    * @param cellId current cell
    * @return nearby cells
    */
  def getNearbyCellsByCell(cellId: String): Array[(Double,Double)] = {
    val enclosingCell = cells(cellId)
    // find nearby grid cells
    getNearbyCellsByEnclosingCell(enclosingCell)
  }

  /**
    * Gets nearby cells by enclosing cell
    *
    * @param enclosingCell
    * @return nearby cells
    */
  def getNearbyCellsByEnclosingCell(enclosingCell: (Double,Double)): Array[(Double,Double)] = {

    val lowLeft = (enclosingCell._1 - SpatialReasoning.LONGITUDE_STEP,enclosingCell._2 - SpatialReasoning.LATITUDE_STEP)
    val midBottom = (enclosingCell._1,enclosingCell._2 - SpatialReasoning.LATITUDE_STEP)
    val lowRight = (enclosingCell._1 + SpatialReasoning.LONGITUDE_STEP,enclosingCell._2 - SpatialReasoning.LATITUDE_STEP)
    val leftCell = (enclosingCell._1 - SpatialReasoning.LONGITUDE_STEP,enclosingCell._2)
    val rightCell = (enclosingCell._1 + SpatialReasoning.LONGITUDE_STEP,enclosingCell._2)
    val topLeft = (enclosingCell._1 - SpatialReasoning.LONGITUDE_STEP,enclosingCell._2 + SpatialReasoning.LATITUDE_STEP)
    val midTop = (enclosingCell._1,enclosingCell._2 + SpatialReasoning.LATITUDE_STEP)
    val topRight = (enclosingCell._1 + SpatialReasoning.LONGITUDE_STEP,enclosingCell._2 + SpatialReasoning.LATITUDE_STEP)
    // nearby cells
    Array(enclosingCell,lowLeft,lowRight,midBottom,midTop,leftCell,rightCell,topLeft,topRight)
      .map(c => (BigDecimal(c._1).setScale(1,BigDecimal.RoundingMode.HALF_UP).toDouble,
        BigDecimal(c._2).setScale(1,BigDecimal.RoundingMode.HALF_UP).toDouble))
      .filter(p => vesselInGrid(p._1,p._2))
  }

  /**
    * Gets nearby ports within 9 cell grid radius
    *
    * @param lon X coordinate of vessel
    * @param lat Y coordinate of vessel
    * @return (nearby ports, contained in grid) tuple
    */
  def getNearbyPorts(lon: Double, lat: Double): (Map[String,(String,String)],Boolean) = {
    val enclosingCell = SpatialReasoning.getEnclosingGridCell(lon,lat)
    // check if it is in the grid
    val isInGrid = gridCells.contains(enclosingCell)
    // if it is out of the grid return empty map
    if (!isInGrid) return (Map(),isInGrid)
    // find nearby grid cells
    val nearbyCellsArray = getNearbyCellsByEnclosingCell(enclosingCell)
    // get cell ids
    val cells = gridCells.filter(gc => nearbyCellsArray.contains(gc._1)).map(_._2).toArray
    // get port ids from cells
    val nearPorts = portsInCells.filter(pc => cells.contains(pc._1)).flatMap(_._2).toArray
    // get nearby ports
    (ports.filter(p => nearPorts.contains(p._1)),isInGrid)
  }

  def getRelevantAreasByCell(lon: Double, lat: Double): Map[String, Polygon] = {

    val enclosingCell = SpatialReasoning.getEnclosingGridCell(lon,lat)

    var areas: List[String] = List()
    if (relevantAreas.contains(enclosingCell)) {
      areas = relevantAreas(enclosingCell).map(_._1).toList
    } else {
      return Map()
    }
    polygonEdges.filterKeys(p => areas.contains(p))
      .mapValues(_.map(y => SpatialReasoning.toNvector(new Coordinate(y._1,y._2))))
      .mapValues(p => p :+ p(0))
      .mapValues(p => new GeometryFactory().createPolygon(p))
      .filter((p) => new IsValidOp(p._2).isValid)
  }

  /**
    * Returns true if object is not close
    * An object is close if it has distance less than 5 miles
    *
    * @param lon X coordinate of vessel
    * @param lat Y coordinate of vessel
    * @return
    */
  def notClose(lon: Double, lat: Double): Boolean = {
    val nearbyPortsResults = getNearbyPorts(lon,lat)
    // get the nearby ports
    val nearbyPorts = nearbyPortsResults._1
    // get the boolean value that says if it is in the grid
    val isInGrid = nearbyPortsResults._2
    // if not in the grid return false, so it won`t pass the recognition
    if (!isInGrid) return false
    // vessel must be far from all the nearby ports
    nearbyPorts.values.forall{
      c =>
        SpatialReasoning.getHarvesineDistance(lon,lat,c._1.toDouble,c._2.toDouble) >= 5.0
    }
  }

  /**
    * Returns true if point lies
    * inside one of the relevant areas
    *
    * @param lon X coordinate of vessel
    * @param lat Y coordinate of vessel
    * @return
    */
  def isInArea(lon: Double, lat: Double): Boolean = {
    // get enclosing cell
    val enclosingCell = SpatialReasoning.getEnclosingGridCell(lon,lat)
    // find relevant areas
    relevantAreas.get(enclosingCell) match {
      case Some(areas) => {
        areas.foreach{
          area =>
            // Tuple values: 2 -> minX, 3 -> maxX, 4 -> minY, 5 -> maxY
            // The MBR (Minimum Bounding Rectangle) is the bounding geometry, formed by the minimum and maximum (X,Y) coordinates
            // specifically coordinates of rectangle are: (MINX MINY), (MAXX MINY), (MAXX MAXY), (MINX MAXY)
            val MBR = Array((area._2.toDouble,area._4.toDouble),(area._3.toDouble,area._4.toDouble),(area._3.toDouble,area._5.toDouble),(area._2.toDouble,area._5.toDouble))
            // if it is inside the MBR area
            if (SpatialReasoning.isInside(MBR,lon,lat)) {
              // check if it is inside the polygon of the relevant area
              if (SpatialReasoning.isInside(polygonEdges(area._1),lon,lat)) return true
            }
        }
        false
      }
        // if no area is found return false
      case None => false
    }
  }

  /**
    * Static grounding for suspicious area
    *
    * @param entities
    * @return
    */
  def groundStatic(entities: Map[String, Iterable[Seq[String]]]): Map[String, Iterable[Seq[String]]] = {
    var newEntities = entities

    val cellIds = getCellIds.map{
      id =>
        Seq(id,true.toString)
    }

    newEntities += ("Suspicious" -> cellIds)
    newEntities
  }

  //def getFishingVessels: Set[String] = fishingVessels

  def getVesselTypes: Map[String,String] = vesselTypes

  def getSpeedTypes: Map[String,(Double,Double,Double)] = speedTypes

  def readFlightLevels(file: String): Unit = {
    val limits = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        val modelId = parts.head
        val fl = (parts(7).toDouble, parts(8).toDouble)
        (modelId,fl)
    }.toMap

    flightLevels = limits
  }

  def getFlightLevel(model:String): Double = {

    val fl = flightLevels.getOrElse(model,(Double.NaN,Double.NaN))
    fl._1
  }

  def getCruiseSpeed(model:String): Double = {

    val fl = flightLevels.getOrElse(model,(Double.NaN,Double.NaN))
    fl._2
  }

  def readFlightPlanTrajectories(file: String): Unit = {
    val trajs = scala.io.Source.fromFile(file).getLines.map{
      line =>
        val parts = line.split("[|]")
        val flightId = parts.head
        val tr = new GeometryFactory().createLineString(parts.tail.map{t => t.split("[,]").takeRight(2).reverse}
          .map{t => SpatialReasoning.toNvector(new Coordinate(t.head.toDouble,t.last.toDouble))})
        (flightId,tr)
    }.toMap

    fp_traj = trajs
  }

  def getFlightPlanTrajectory(model:String): LineString = fp_traj.getOrElse(model,
    new GeometryFactory().createLineString(Array.empty[Coordinate]))

  def constructTopOfCD(topcd: Map[String,List[Seq[(String,String,String,String,Long)]]],
                       dict: Iterable[Predicate.GroundingDict],
                       aircraftId:String, altitude: String): Iterable[Predicate.GroundingDict] = {

    var tc = new ListBuffer[(Double,Long)]()
    var td = new ListBuffer[(Double,Long)]()

    topcd.foreach { top =>
      top._2.foreach { q =>

        if (!_topOfClimb.contains(top._1) && q.head._3.toDouble <= q(1)._3.toDouble && q(1)._3.toDouble < q(2)._3.toDouble &&
          math.round(q(2)._3.toDouble/200) * 200 == math.round(q(3)._3.toDouble/200) * 200 &&
          math.round(q(3)._3.toDouble/200) * 200 == math.round(q.last._3.toDouble/200) * 200) {

          tc = tc :+ (q(2)._3.toDouble,q(2)._5)

        } else if (!_topOfDescent.contains(top._1) && math.round(q.head._3.toDouble/200) * 200 == math.round(q(1)._3.toDouble/200) * 200 &&
          math.round(q(1)._3.toDouble/200) * 200 == math.round(q(2)._3.toDouble/200) * 200 &&
          q(2)._3.toDouble > q(3)._3.toDouble && q(3)._3.toDouble >= q.last._3.toDouble)
        {

          td = td :+ (q(2)._3.toDouble,q(2)._5)
        }
      }
      if (tc.nonEmpty) {
        val tcp = tc.minBy(v => math.abs(getFlightLevel(top._1)-v._1))
        topOfCD ++= Iterable((List(top._1), Map(aircraftId -> top._1, altitude -> tcp._1.toString, "$Dir" -> "up"),
          Map[String, Intervals](), Map("T" -> Set(tcp._2))))
        _topOfClimb += top._1 -> tcp._1
        tc.clear()
      }

      if (td.nonEmpty) {
        val tdp = td.minBy(v => math.abs(getFlightLevel(top._1)-v._1))
        topOfCD ++= Iterable((List(top._1), Map(aircraftId -> top._1, altitude -> tdp._1.toString, "$Dir" -> "down"),
          Map[String, Intervals](), Map("T" -> Set(tdp._2))))
        _topOfDescent += top._1 -> tdp._1
        td.clear()
      }
    }
    //println(topOfCD)
    topOfCD
  }

  def getRecTopOfDescent(flight: String): Boolean = {
    _topOfDescent.contains(flight)
  }

  def getTopOfC: Iterable[Predicate.GroundingDict] = {
    topOfCD.filter(v => v._2("$Dir") != "down")
  }

  def getTopOfD: Iterable[Predicate.GroundingDict] = {
    val topd = topOfCD.filter(v => v._2("$Dir") != "up")
    topOfCD = Iterable()
    topd
  }

  def computeAngle3d(aircraftId:String, points: List[Seq[(String,String,String,Int)]]): List[Seq[(String,String,String,Int)]] = {
    points.filter { p =>
      if (p.size > 2 && p(1)._3.toDouble > getFlightLevel(aircraftId) - 5000) {
        true
        /*val v0 = Seq(p(1)._1.toDouble - p.head._1.toDouble,p(1)._2.toDouble - p.head._2.toDouble,
          p(1)._3.toDouble - p.head._3.toDouble)
        val v1 = Seq(p(2)._1.toDouble - p(1)._1.toDouble,p(2)._2.toDouble - p(1)._2.toDouble,
          p(2)._3.toDouble - p(1)._3.toDouble)
        val angle = math.atan2(norm(crossProduct3d(v0,v1)),dotProduct3d(v0,v1)).toDegrees
        //println(angle)
        angle > 85.0 && angle < 95.0*/
      } else false
    }
  }

  def computeAngle2d(aircraftId:String, points: List[Seq[(String,String,String,String,Long)]]): List[Seq[(String,String,String,String,Long)]] = {

    /*var new_points =  new ListBuffer[(Seq[(String,String,String,Int)],Double)]()
    points.foreach { p =>
      if (p.size > 4 && p(2)._3.toDouble > getFlightLevel(aircraftId) - 5000) {
        val v0 = Seq(p(1)._4.toDouble - p(2)._4.toDouble,p(1)._3.toDouble - p(2)._3.toDouble,0.0)
        val v1 = Seq(p(3)._4.toDouble - p(2)._4.toDouble,p(3)._3.toDouble - p(2)._3.toDouble,0.0)
        //        val v2 = Seq(p(2)._3.toDouble - p.head._3.toDouble,p(2)._4.toDouble - p.head._4.toDouble)
        new_points += ((p, math.atan2(norm3d(crossProduct(v0,v1)),dotProduct3d(v0,v1)).toDegrees))
      }
    }

    new_points.sliding(2).toList.filter {an => an.head._2 >= 170.0 && an.last._2 < 170.0}.map(_.last._1)*/

    points.filter { p =>
      if (p.size > 4 && p(2)._3.toDouble > getFlightLevel(aircraftId) - 4000) {
        val v0 = Seq(p(1)._5.toDouble - p(2)._5.toDouble,(math.round(p(1)._3.toDouble/200.0)*200.0) -
          (math.round(p(2)._3.toDouble/200.0)*200.0),0.0)
        val v1 = Seq(p(3)._5.toDouble - p(2)._5.toDouble,(math.round(p(3)._3.toDouble/200.0)*200.0) -
          (math.round(p(2)._3.toDouble/200.0)*200.0),0.0)
        //        val v2 = Seq(p(2)._3.toDouble - p.head._3.toDouble,p(2)._4.toDouble - p.head._4.toDouble)
        val angle = math.atan2(norm3d(crossProduct(v0,v1)),dotProduct3d(v0,v1)).toDegrees
        angle > 80.0 && angle < 170.0
      } else false
    }
  }

  def crossProduct(v1:Seq[Double], v2:Seq[Double]): Seq[Double] = {

    Seq(v1(1)*v2(2)-v1(2)*v2(1), v1(2)*v2.head-v1.head*v2(2), v1.head*v2(1)-v1(1)*v2.head)
  }

  def dotProduct3d(v1:Seq[Double], v2:Seq[Double]): Double = {

    v1.head*v2.head + v1(1)*v2(1) + v1(2)*v2(2)
  }

  def norm3d(v:Seq[Double]): Double = math.sqrt(math.pow(v.head,2)+math.pow(v(1),2)+math.pow(v(2),2))

  def dotProduct2d(v1:Seq[Double], v2:Seq[Double]): Double = {

    v1.head*v2.head + v1.last*v2.last
  }

  def norm2d(v:Seq[Double]): Double = math.sqrt(math.pow(v.head,2)+math.pow(v.last,2))

}
