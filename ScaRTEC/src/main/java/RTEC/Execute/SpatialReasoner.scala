package RTEC.Execute

import java.io

import RTEC.Data.{ExtraLogicReasoning, SpatialReasoning}
import RTEC.Data
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, _}
import com.vividsolutions.jts.operation.distance.DistanceOp

import scala.collection.immutable.ListMap
import scala.collection.SortedMap
import scala.collection.mutable.ListBuffer

/**
  * Created by eftsilio on 13/2/2017.
  */
class SpatialReasoner {

  // dynamic parameters
  private var vessel_coords: Map[String, SortedMap[Int,Seq[String]]] = Map()
  private var vessel_vel: Map[String, SortedMap[Int,Seq[String]]] = Map()
  private var vessel_gaps: Map[String, SortedMap[Int,Seq[String]]] = Map()
  private var projections: Map[String,Map[Int,(Seq[String], Seq[String])]] = Map()
  private var trajectories: Map[String,Map[Data.Intervals,Seq[String]]] = Map()
  private var trajectories_cells: Map[String,Map[Data.Intervals,Seq[(Double,Double)]]] = Map()
  private var vessels_areas: Map[(String,String),Seq[(String,Int)]] = Map()
  private var proximity_vessels: Map[(String,String),Seq[(Int,Int)]] = Map()

  // static parameters
  private var _start: Int = _
  private var _end: Int = _
  private var _input: (Seq[Reader.Main.InputHappensAt]) = _
  private var _lastTime: Int = _

  def run(input: (Seq[Reader.Main.InputHappensAt], Seq[Reader.Main.InputHoldsAt], Seq[Reader.Main.InputHoldsFor]),
          outputFile: String,
          start: Int, end: Int, lastTime: Int): Long = {

    _input = input._1
    _start = start
    _end = end
    _lastTime = lastTime

    // Update Input data
    processInput()

    val s = System.currentTimeMillis

    createProjections()

    createTrajectories()

    checkIntersectionsAreas()

    checkProximity()


    /* ============= Maritime Domain ============= */
    //if (ExtraLogicReasoning.getCellIds.nonEmpty)
    //  _windowDB.createInCellFluents
    /* ============= Maritime Domain ============= */

    val e = System.currentTimeMillis
    val recognitionTime = e - s

    // Write results for this window
    writeResults(outputFile,false)

    if (_end >= _lastTime) writeResults(outputFile,true)

    // return recognition time
    recognitionTime
  }

  private def processInput(): Unit = {

    // Process HappensAt input
    _input foreach { ie =>
      if (getEventType(ie)) {
        val id: String = ie._2.head
        val te: Seq[String] = ie._1.name +: ie._2.tail
        val time: Int = ie._3
        if (te.head == "coord") {
          val m: SortedMap[Int, Seq[String]] = vessel_coords.getOrElse(id,SortedMap())
          vessel_coords += (id -> (m + (time -> te)))
          }
        else if (te.head == "velocity") {
          //println(ie)
          val m: SortedMap[Int, Seq[String]] = vessel_vel.getOrElse(id,SortedMap())
          vessel_vel += (id -> (m + (time -> te)))
        }
        else {
          val m: SortedMap[Int, Seq[String]] = vessel_gaps.getOrElse(id,SortedMap())
          vessel_gaps += (id -> (m + (time -> te)))
        }
      }
    }
  }

  private def getEventType(event: (Data.InstantEventId, Seq[String], Int)): Boolean = {

    val typeOfEvent = event._1
    typeOfEvent.name match {
      case "coord" => true
      case "velocity" => true
      case "gap_start" => true
      case _ => false
    }

  }

  private def createProjections(): Unit = {

    vessel_coords foreach {
      x =>
        val lastTime: Int = x._2.lastKey
        val gap_v: Boolean = vessel_gaps.contains(x._1)
        val gap_l: Boolean = gap_v match {
          case true => vessel_gaps(x._1).contains(lastTime)
          case false => false
        }
        if (! gap_l) {
          if (lastTime < _end) {

            //println(vessel_vel(x._1))

            val speed: Double = vessel_vel(x._1)(lastTime)(1).toDouble
            val heading: Double = vessel_vel(x._1)(lastTime)(2).toDouble.toRadians
            if (speed != 0.0) {
              val lon: Double = x._2(lastTime)(1).toDouble.toRadians
              val lat: Double = x._2(lastTime)(2).toDouble.toRadians
              val delta: Double = (_end - lastTime) * speed / 3600 / 6373
              //val new_lon: Double = BigDecimal(Math.asin((Math.sin(lon) * Math.cos(delta)) + (Math.cos(lon) * Math.sin(delta) *
              //  Math.cos(heading))).toDegrees).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
              //val new_lat: Double = BigDecimal((lat + Math.atan2(Math.sin(heading) * Math.sin(delta) * Math.cos(lon),
              //  Math.cos(delta) - (Math.sin(lon) * Math.sin(new_lon.toRadians)))).toDegrees).setScale(6,
              //  BigDecimal.RoundingMode.HALF_UP).toDouble

              val new_lat: Double = BigDecimal(Math.asin((Math.sin(lat) * Math.cos(delta)) + (Math.cos(lat) * Math.sin(delta) *
                Math.cos(heading))).toDegrees).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
              val new_lon: Double = BigDecimal((lon + Math.atan2(Math.sin(heading) * Math.sin(delta) * Math.cos(lat),
                Math.cos(delta) - (Math.sin(lat) * Math.sin(new_lat.toRadians)))).toDegrees).setScale(6,
                BigDecimal.RoundingMode.HALF_UP).toDouble

              //val (llx, lly): (Double,Double) = SpatialReasoning.getEnclosingGridCell(new_lon,new_lat)
              if (ExtraLogicReasoning.vesselInGrid(new_lon, new_lat)) {
                vessel_coords += (x._1 -> (vessel_coords(x._1) + (_end -> List("coord", new_lon.toString,
                  new_lat.toString))))
                vessel_vel += (x._1 -> (vessel_vel(x._1) + (_end -> List("velocity", speed.toString, heading.toString))))
                projections += (x._1 -> Map(_end -> (Seq("coord",new_lon.toString,new_lat.toString),
                  vessel_vel(x._1)(lastTime))))
              } else {
                projections += (x._1 -> Map(lastTime -> (vessel_coords(x._1)(lastTime),
                  vessel_vel(x._1)(lastTime))))
              }
            } else {
              projections += (x._1 -> Map(_end -> (vessel_coords(x._1)(lastTime),
                vessel_vel(x._1)(lastTime))))
            }
          } else {
            try {
              projections += (x._1 -> Map(_end -> (vessel_coords(x._1)(lastTime),
                vessel_vel(x._1)(lastTime))))
            }
            catch {
              case e: Exception => println(s"WARNING: ${e.getMessage}")
            }
          }
        }
    }
  }

  private def createTrajectories(): Unit = {

    vessel_coords foreach { x => val m = x._2.toList
      var j : SortedMap[(Int,Int), Seq[String]] = SortedMap()
      for (i <- 0 until m.length-1 if !(vessel_gaps.contains(x._1) && vessel_gaps(x._1).contains(m(i)._1))){
        j += (m(i)._1, m(i+1)._1) -> List(m(i)._2.drop(1), m(i+1)._2.drop(1)).distinct.flatten
      }
      if (j.nonEmpty) {
        trajectories += x._1 -> ListMap(j.groupBy(_._2).mapValues(_.keys.toList).map(_.swap)
          .map { case (key, value) => Data.Intervals(Vector((key.head._1, key.last._2))) -> value }
          .toSeq.sortBy(_._1.head): _*)//.transform((k,v) => (Data.Intervals(k.toVector),v))
      }
    }
    trajectories_cells = trajectories.mapValues(_.mapValues(y => y.grouped(2).map(e =>
      (e.head.toDouble,e.last.toDouble)).toList)
        .mapValues(ce =>
          if (ce.length > 1) {
            SpatialReasoning.getCrossingCells(ce)
          } else {
            if (ExtraLogicReasoning.vesselInGrid(ce.head._1,ce.head._2)) {
              Seq(SpatialReasoning.getEnclosingGridCell(ce.head._1, ce.head._2))
            } else Seq()
          }))
  }

  private def checkIntersectionsAreas(): Unit = {

    trajectories foreach { x =>
      val vessel_ID = x._1
      x._2 foreach{ y =>
        val time = y._1
        val points = y._2
        if (points.length > 2) {

          val ptc = Array(SpatialReasoning.toNvector(new Coordinate(points.head.toDouble, points(1).toDouble)),
            SpatialReasoning.toNvector(new Coordinate(points(2).toDouble, points(3).toDouble)))

          val poly = {ExtraLogicReasoning.getRelevantAreasByCell(points.head.toDouble, points(1).toDouble) ++
            ExtraLogicReasoning.getRelevantAreasByCell(points(2).toDouble, points(3).toDouble)}

          val line: LineString = new GeometryFactory().createLineString(ptc)

          poly.foreach {p =>
            try {
              val common = line.intersection(p._2)
              if (!common.isEmpty) {
                val numgeo = common.getNumGeometries
                for (g <- 0 until numgeo if common.getGeometryN(g).getGeometryType != "Point") {
                  val part = common.getGeometryN(g)
                  val inter_coords = part.getCoordinates
                  val lip = estimateTimeOfInter(inter_coords, ptc, time, p._2)

                  vessels_areas += (vessel_ID, p._1) ->
                    (vessels_areas.getOrElse((vessel_ID, p._1), Seq()) ++ lip)

                } /*else {
                vessels_areas += (vessel_ID, p._1) ->
                  (vessels_areas.getOrElse((vessel_ID, p._1), Seq()) ++ Seq(("in", time.head), ("in", time.last)))
              }*/
              }
            }
            catch {
              case e: Exception =>
            }
          }
        } else {

          val ptc = SpatialReasoning.toNvector(new Coordinate(points.head.toDouble, points(1).toDouble))

          val poly = ExtraLogicReasoning.getRelevantAreasByCell(points.head.toDouble, points(1).toDouble)

          val singlePoint: Point = new GeometryFactory().createPoint(ptc)

          poly.foreach {p =>
            if (singlePoint.intersects(p._2) && !singlePoint.touches(p._2)) {
              vessels_areas += (vessel_ID,p._1) ->
                (vessels_areas.getOrElse((vessel_ID,p._1),Seq()) ++ Seq(("in",time.head),("in",time.last)))
            }
          }
        }
      }
    }
  }

  private def checkProximity(): Unit = {

    var continuous_traj: Map[String, Map[Data.Intervals, List[(Double, Double)]]] = Map()
    trajectories.foreach {
      x =>
        continuous_traj += x._1 -> x._2.keys.toList.map(t => (t.head, t.last)).
          init.foldRight(Vector(Vector((x._2.keys.last.head, x._2.keys.last.last)))) { (tup, acc) =>
          if (acc.head.head._1 != tup._2)
            Vector(tup) +: acc // gap too big, start new sub-List
          else
            (tup +: acc.head) +: acc.tail // prepend to current sub-List
        }.map(y => Data.Intervals(y) -> y.flatMap(ti => trajectories_cells(x._1)(Data.Intervals(Vector(ti)))).toList
          .distinct.flatMap(ce => ExtraLogicReasoning.getNearbyCellsByEnclosingCell(ce)).distinct).toMap
    }

    val combs = continuous_traj.keys.toList.combinations(2)
    combs.foreach {
      v =>
        val v1_cells = continuous_traj(v.head).values.flatten.toSet
        val v2_cells = continuous_traj(v.last).values.flatten.toSet
        val spatial_intersection = v1_cells.intersect(v2_cells)

        if (spatial_intersection.nonEmpty) {
          val v1_time = spatial_intersection.map(si => continuous_traj(v.head)
            .find(_._2.contains(si)).head._1).toVector.sortBy(_.head).head
          val v2_time = spatial_intersection.map(si => continuous_traj(v.last)
            .find(_._2.contains(si)).head._1).toVector.sortBy(_.head).head
          
          val time_intersection = v1_time.&(v2_time).t

          if (time_intersection.nonEmpty) {

            val time_unify = time_intersection.init.foldRight(List(List(time_intersection.last))) { (int, acc) =>
              if (acc.head.head._1 != int._2)
                List(int) :: acc
              else
                (int :: acc.head) ::acc.tail//List((int._1, acc.head.last._2)) :: acc.tail
            }

            time_unify foreach { ti =>
              ti foreach { t =>
                val v1_segment = transformSegments(trajectories(v.head).filterKeys(k => k.head <= t._1 && k.last >= t._2)
                    .mapValues(_.map(_.toDouble)).toList, t)
                val v2_segment = transformSegments(trajectories(v.last).filterKeys(k => k.head <= t._1 && k.last >= t._2)
                  .mapValues(_.map(_.toDouble)).toList, t)

                if (DistanceOp.isWithinDistance(v1_segment,v2_segment,0.1/6373)) {

                  val cpa = closestPointOfApproach(v1_segment.getCoordinates.toList, v2_segment.getCoordinates.toList, t)
                  //println(cpa)

                  if (cpa.nonEmpty) {
                    proximity_vessels += (v.head,v.last) -> (proximity_vessels
                      .getOrElse((v.head,v.last),Seq()) ++ cpa)
                  }
                }
              }
            }
          }
        }
    }
  }

  def closestPointOfApproach(v1_c:List[Coordinate], v2_c:List[Coordinate], time: (Int,Int)): List[(Int,Int)] = {

    val Dt = time._2 - time._1

    val v1_v = ((v1_c.last.x - v1_c.head.x)/ Dt, (v1_c.last.y - v1_c.head.y)/ Dt, (v1_c.last.z - v1_c.head.z)/ Dt)
    val v2_v = ((v2_c.last.x - v2_c.head.x)/ Dt, (v2_c.last.y - v2_c.head.y)/ Dt, (v2_c.last.z - v2_c.head.z)/ Dt)

    var tcpa: Double = 0
    val denominator = math.pow(v1_v._1 - v2_v._1,2) + math.pow(v1_v._2 - v2_v._2,2) + math.pow(v1_v._3 - v2_v._3,2)
    if (denominator != 0) {
      tcpa = -(((v1_c.head.x - v2_c.head.x) * (v1_v._1 - v2_v._1)) + ((v1_c.head.y - v2_c.head.y) * (v1_v._2 - v2_v._2))
        + ((v1_c.head.z - v2_c.head.z) * (v1_v._3 - v2_v._3))) / denominator
    } else {
      tcpa = 0
    }

    var ftcpa: ListBuffer[Int] = ListBuffer()

    val l1 = SpatialReasoning.toLatLon(v1_c.head,v1_v,math.round(tcpa))
    val l2 = SpatialReasoning.toLatLon(v2_c.head,v2_v,math.round(tcpa))

    val cpa = BigDecimal(SpatialReasoning.getHarvesineDistance(l1._1,l1._2,l2._1,l2._2))
      .setScale(3,BigDecimal.RoundingMode.HALF_UP)

    if (cpa == 0.1 && tcpa > 0 && tcpa < Dt) {
      ftcpa += (time._1 + math.round(tcpa).toInt,time._1 + (math.round(tcpa)+1).toInt)
    } else if (cpa == 0.1 && tcpa == 0) {
      ftcpa += (time._1,time._2)
    } else if (cpa < 0.1) {

      val dist = math.pow(0.1/6373,2)

      val a = math.pow(v1_v._1 - v2_v._1,2) + math.pow(v1_v._2 - v2_v._2,2) + math.pow(v1_v._3 - v2_v._3,2)
      val b = 2*(((v1_c.head.x - v2_c.head.x) * (v1_v._1 - v2_v._1)) + ((v1_c.head.y - v2_c.head.y) * (v1_v._2 - v2_v._2))
        + ((v1_c.head.z - v2_c.head.z) * (v1_v._3 - v2_v._3)))
      val c = (math.pow(v1_c.head.x - v2_c.head.x,2) + math.pow(v1_c.head.y - v2_c.head.y,2)
        + math.pow(v1_c.head.z - v2_c.head.z,2)) - dist

      val discriminant = math.pow(b,2) - (4 * a * c)

      val roots = List((-b - math.sqrt(discriminant)) / (2*a), (-b + math.sqrt(discriminant)) / (2*a)).sorted

      if (!(roots.head.isNaN && roots.last.isNaN)) {

        if (roots.head < 0) {

          val l3 = SpatialReasoning.toLatLon(v1_c.head, v1_v, 0.0)
          val l4 = SpatialReasoning.toLatLon(v2_c.head, v2_v, 0.0)

          val start_points_dist = BigDecimal(SpatialReasoning.getHarvesineDistance(l3._1, l3._2, l4._1, l4._2))
            .setScale(3, BigDecimal.RoundingMode.HALF_UP)
          if (start_points_dist <= 0.1) ftcpa += time._1

        } else if (roots.head >= 0 && roots.head <= Dt) {

          ftcpa += time._1 + math.floor(roots.head).toInt
        }

        if (roots.last >= 0 && roots.last <= Dt) {

          ftcpa += time._1 + math.ceil(roots.last).toInt
        } else if (roots.last > Dt && ftcpa.nonEmpty) ftcpa += time._2
      } else {
        ftcpa += (time._1,time._2)
      }
      //println(cpa,tcpa,ftcpa,roots,time)
    }
    ftcpa.grouped(2).collect{ case Seq(a, b) =>  (a, b) }.toList
  }

  def transformSegments(endpoints: List[(Data.Intervals,Seq[Double])], time: (Int,Int)): Geometry = {

    if (endpoints.head._2.length < 3) {
      new GeometryFactory().createPoint(SpatialReasoning
        .toNvector(new Coordinate(endpoints.head._2.head,endpoints.head._2.last)))
    } else {
      val v3d1 = SpatialReasoning.toNvector(new Coordinate(endpoints.head._2.head,endpoints.head._2(1)))
      val v3d2 = SpatialReasoning.toNvector(new Coordinate(endpoints.head._2(2),endpoints.head._2.last))

      val vx = (v3d2.x - v3d1.x) / (endpoints.head._1.last - endpoints.head._1.head)
      val vy = (v3d2.y - v3d1.y) / (endpoints.head._1.last - endpoints.head._1.head)
      val vz = (v3d2.z - v3d1.z) / (endpoints.head._1.last - endpoints.head._1.head)

      val ts = time._1 - endpoints.head._1.head
      val ps = new Coordinate(v3d1.x + ts * vx, v3d1.y + ts * vy, v3d1.z + ts * vz)

      val te = time._2 - endpoints.head._1.head
      val pe = new Coordinate(v3d1.x + te * vx, v3d1.y + te * vy, v3d1.z + te * vz)

      new GeometryFactory().createLineString(Array(ps,pe))
    }
  }

  /**
    * Estimate time of intersection
    *
    * @return
    */

  def estimateTimeOfInter(coord: Array[Coordinate], endpoints: Array[Coordinate], time: Data.Intervals, poly: Polygon):
    List[(String,Int)] =
  {
    var lip: ListBuffer[(String,Int)] = ListBuffer()

    val Dt = time.last - time.head

    val vx = (endpoints.last.x - endpoints.head.x) / Dt

    val ts = (coord.head.x - endpoints.head.x)/vx
    val te = (coord.last.x - endpoints.head.x)/vx

    val ftip = (math.round(time.head + ts).toInt, math.round(time.head + te).toInt)

    if (coord.last.equals3D(endpoints.last)) {
      lip +=(("in", ftip._1), ("in", ftip._2))
    } else if (coord.head.equals3D(endpoints.head)) {
      lip += (("in",ftip._1),("left",ftip._2))
    } else {
      lip += (("in",ftip._1),("left",ftip._2))
    }
    lip.toList
  }

  /**
    * Gets last point of trajectory
    *
    * @return
    */
  def getProjections: Map[String,Map[Int,(Seq[String], Seq[String])]] = projections

  def getProximity: Map[(String,String),Seq[(Int,Int)]] = proximity_vessels

  /**
    * Updates trajectories
    */
  def updateTrajectories(traj: Map[String,Map[Int,(Seq[String], Seq[String])]]): Unit = {

    vessel_coords = traj.map {x => x._1 -> SortedMap(x._2.head._1 -> x._2.head._2._1)}
    vessel_vel = traj.map {x => x._1 -> SortedMap(x._2.head._1 -> x._2.head._2._2)}
  }

  def updateProximity(prox: Map[(String,String),Seq[(Int,Int)]]): Unit = {

    proximity_vessels = prox
  }


  def writeResults(outputFile: String, wprox: Boolean): Unit = {

    val fd = new io.FileWriter(outputFile, true)
    if (!wprox) {
      vessels_areas = vessels_areas.mapValues(_.distinct)
      vessels_areas.foreach{ va =>
        va._2.foreach{vl =>
          if (vl._1 == "in") {fd.write(s"${va._1._1},${vl._2},${va._1._2},isInArea" + "\n")
          } else {fd.write(s"${va._1._1},${vl._2},${va._1._2},leavesArea" + "\n")}
        }
      }
      vessels_areas = Map()
    } else {

      proximity_vessels = proximity_vessels.mapValues(p => Data.Intervals(p.toVector)).mapValues(p => p.|(p).t.toList)
      proximity_vessels.foreach{vv =>
        vv._2.foreach{ vl => fd.write(s"${vv._1._1},${vl._2},${vl._1},${vv._1._2},proximity" + "\n")
          }
        }
    }
    fd.close()
  }

}