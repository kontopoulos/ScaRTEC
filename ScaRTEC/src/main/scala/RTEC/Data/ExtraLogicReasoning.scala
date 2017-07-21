package RTEC.Data

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Polygon}
import com.vividsolutions.jts.operation.valid.IsValidOp

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

}
