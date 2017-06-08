package RTEC.Data

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, LineString, Polygon}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @author Kontopoulos Ioannis
  */
object SpatialReasoning {

  // grid starting X point
  val LONGITUDE_GRID_START = -10.0//19.884766
  // grid starting Y point
  val LATITUDE_GRID_START = 45.0//34.695777
  // step to move on to the next horizontal grid cell
  val LONGITUDE_STEP = 0.1//0.25
  // step to move on to the next vertical grid cell
  val LATITUDE_STEP = 0.1//0.25
  // assumption: Lon/Lat have at most 6 decimal digits
  val MU = 1000000

  /**
    * This uses the ‘haversine’ formula to calculate
    * the great-circle distance between two points – that is,
    * the shortest distance over the earth’s surface – giving an ‘as-the-crow-flies’ distance
    * between the points
    * (ignoring any hills they fly over, of course)
    *
    * @param lon1 longitude 1
    * @param lat1 latitude 1
    * @param lon2 longitude 2
    * @param lat2 latitude 2
    * @return distance in miles
    */
  def getHarvesineDistance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val lon1Rad = lon1*(Math.PI/180)
    val lat1Rad = lat1*(Math.PI/180)
    val lon2Rad = lon2*(Math.PI/180)
    val lat2Rad = lat2*(Math.PI/180)
    val dLon = lon2Rad - lon1Rad
    val dLat = lat2Rad - lat1Rad
    val a = Math.pow(Math.sin(dLat/2),2) + Math.cos(lat1Rad)*Math.cos(lat2Rad)*Math.pow(Math.sin(dLon/2),2)
    val c = 2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a))
    // earth radius = 3961 miles / 6373 km
    6373*c
  }

  /**
    * Returns the enclosing cell based on the coordinates given
    *
    * @param lon longitude
    * @param lat latitude
    * @return enclosing cell
    */
  def getEnclosingGridCell(lon: Double, lat: Double): (Double,Double) = {
    val x = Math.round(lon * MU)
    val y = Math.round(lat * MU)
    val xStart = Math.round(LONGITUDE_GRID_START * MU)
    val xStep = Math.round(LONGITUDE_STEP * MU)
    val yStart = Math.round(LATITUDE_GRID_START * MU)
    val yStep = Math.round(LATITUDE_STEP * MU)
    val llx = x - ((x - xStart) % xStep)
    val lly = y - ((y - yStart) % yStep)
    val lowLeftX = llx.toDouble / MU
    val lowLeftY = lly.toDouble / MU
    (lowLeftX,lowLeftY)
  }

  def getCrossingCells(coords: Seq[(Double,Double)]): Seq[(Double,Double)] = {

    val c1 = getEnclosingGridCell(coords.head._1,coords.head._2)
    val c2 = getEnclosingGridCell(coords.last._1,coords.last._2)

    if (c1 != c2) {

      var cells = new ListBuffer[(Double,Double)]()
      val minX = Math.min(c1._1, c2._1)
      val maxX = Math.max(c1._1, c2._1)
      val minY = Math.min(c1._2, c2._2)
      val maxY = Math.max(c1._2, c2._2)

      val ptc = Array(new Coordinate(coords.head._1, coords.head._2),
        new Coordinate(coords.last._1, coords.last._2))

      val Poly = createTrajGrid(minX, maxX, minY, maxY)

      val line: LineString = new GeometryFactory().createLineString(ptc)

      Poly.foreach {
        p =>
          if (line.crosses(p)) {
            if (ExtraLogicReasoning.vesselInGrid(p.getCoordinate.getOrdinate(0), p.getCoordinate.getOrdinate(1))) {
              cells += ((p.getCoordinate.getOrdinate(0), p.getCoordinate.getOrdinate(1)))
            }
          }
      }
      cells.toList
    } else {
      Seq(c1)
    }
  }

  def createTrajGrid(minX: Double, maxX: Double, minY: Double, maxY: Double): Array[com.vividsolutions.jts.geom.Polygon] =
  {
    val Poly: ArrayBuffer[com.vividsolutions.jts.geom.Polygon] = ArrayBuffer()
    for (i <- minX*MU to(maxX*MU, LONGITUDE_STEP*MU)) {
      for (j <- minY * MU to(maxY*MU, LATITUDE_STEP*MU)) {

        val vertices = Array(new Coordinate(i / MU, j / MU), new Coordinate((i + LONGITUDE_STEP * MU) / MU, j / MU),
          new Coordinate((i + LONGITUDE_STEP * MU) / MU, (j + LATITUDE_STEP * MU) / MU),
          new Coordinate(i / MU, (j + LATITUDE_STEP * MU) / MU))

        Poly += new GeometryFactory().createPolygon(vertices :+ vertices(0))
      }
    }
    Poly.toArray
  }

  def toNvector(v_c:Coordinate): Coordinate = {

    new Coordinate(math.cos(v_c.y.toRadians)*math.sin(v_c.x.toRadians),
      math.cos(v_c.y.toRadians)*math.cos(v_c.x.toRadians), math.sin(v_c.y.toRadians))
  }

  def toLatLon(n:Coordinate, v:(Double,Double,Double), t:Double): (Double,Double) = {

    val j = (n.x+(t*v._1), n.y+(t*v._2), n.z+(t*v._3))
    (BigDecimal(math.atan2(j._1,j._2).toDegrees).setScale(6,BigDecimal.RoundingMode.HALF_UP).toDouble,
      BigDecimal(math.atan2(j._3,math.sqrt(math.pow(j._1,2)+math.pow(j._2,2))).toDegrees)
        .setScale(6,BigDecimal.RoundingMode.HALF_UP).toDouble)

  }

  /**
    * Implementation of the ray-casting algorithm
    * for determining if a point is inside a polygon
    *
    * @param polygon array of polygon points coordinates
    * @param lon longitude
    * @param lat latitude
    * @return true if inside the given area
    */
  def isInside(polygon: Array[(Double,Double)], lon: Double, lat: Double): Boolean = {
    val x = lon
    val y = lat
    val n = polygon.length
    var inside = false
    var (p1x,p1y) = polygon.head
    for (i <- 0 to n) {
      val (p2x,p2y) = polygon(i % n)
      if (y > Math.min(p1y,p2y)) {
        if (y <= Math.max(p1y,p2y)) {
          if (x <= Math.max(p1x,p2x)) {
            var xIntersection = -1.0
            if (p1y != p2y) {
              xIntersection = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
            }
            if (p1x == p2x || x <= xIntersection) {
              inside = !inside
            }
          }
        }
      }
      p1x = p2x
      p1y = p2y
    }
    inside
  }

  /**
    * Checks if a vessel has heading
    * towards another vessel
    *
    * @param lon1
    * @param lat1
    * @param heading1
    * @param lon2
    * @param lat2
    * @return
    */
  def hasHeading(lon1: Double, lat1: Double, heading1: Double, lon2: Double, lat2: Double): Boolean = {
    val delta = 0.1
    // line passing through (lon1,lat1), tan = 90-heading
    // y = ax+b, a = 1/tan(heading1), b = y - a*x = lat1 - a*lon1
    //val a = 1/Math.tan(heading1*Math.PI/180)
    //val b = lat1 - a*lon1
    // line l passing through (lonl,latl)=(lon1,lat1), tanl = tan - 5
    val al = 1/Math.tan((heading1-5)*Math.PI/180)
    val bl = lat1 - al*lon1
    // line g passing through (long,latg)=(lon1,lat1), tanl = tan + 5
    val ag = 1/Math.tan((heading1+5)*Math.PI/180)
    val bg = lat1 - al*lon1
    var isHeading = false
    if (heading1 < 180.0) {
      if (lon2 > lon1) {
        // intersection of line x=lon2+Delta with line l
        val x1lon = lon2 + delta
        val y1lon = al * x1lon + bl
        // intersection of line x=lon2+Delta with line g
        val xglon = lon2 + delta
        val yglon = ag * xglon + bg
        isHeading = isInside(Array((lon1,lat1),(x1lon,y1lon),(xglon,yglon)),lon2,lat2)
      }
    }
    else {
      if (lon1 > lon2) {
        // intersection of line x=lon2-Delta with line l
        val x1lon = lon2 - delta
        val y1lon = al * x1lon + bl
        // intersection of line x=lon2-Delta with line g
        val xglon = lon2 - delta
        val yglon = ag * xglon + bg
        isHeading = isInside(Array((lon1,lat1),(x1lon,y1lon),(xglon,yglon)),lon2,lat2)
      }
    }
    isHeading
  }

  /* PYTHON CODE
  * def checkHeading(lon1, lat1, heading1, lon2, lat2):
    delta = 0.1
    # line passing through (lon1,lat1), tan = 90-heading
    # y = ax+b, a = 1/tan(heading1), b = y - a*x = lat1 - a*lon1
    a = 1/tan(heading1*pi/180)
    b = lat1 - a*lon1
    # line l passing through (lonl,latl)=(lon1,lat1), tanl = tan - 5
    al = 1/tan((heading1-5)*pi/180)
    bl = lat1 - al*lon1
    # line g passing through (long,latg)=(lon1,lat1), tanl = tan + 5
    ag = 1/tan((heading1+5)*pi/180)
    bg = lat1 - ag*lon1
    isHeading = False
    if (heading1<180):
        if (lon2>lon1):
            # intersection of line x=lon2+Delta with line l
            x_l_lon = lon2+delta
            y_l_lon = al*x_l_lon+bl
            # intersection of line x=lon2+Delta with line g
            x_g_lon = lon2+delta
            y_g_lon = ag*x_g_lon+bg
            isHeading = pointInArea(lon2,lat2,[(lon1,lat1),(x_l_lon,y_l_lon),(x_g_lon,y_g_lon)])
    else:
        if (lon1>lon2):
            # intersection of line x=lon2-Delta with line l
            x_l_lon = lon2-delta
            y_l_lon = al*x_l_lon+bl
            # intersection of line x=lon2-Delta with line g
            x_g_lon = lon2-delta
            y_g_lon = ag*x_g_lon+bg
            isHeading = pointInArea(lon2,lat2,[(lon1,lat1),(x_l_lon,y_l_lon),(x_g_lon,y_g_lon)])
    return isHeading
  * */

}
