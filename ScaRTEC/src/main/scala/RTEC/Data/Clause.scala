package RTEC.Data

import RTEC.Execute.EventDB
import RTEC._
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import com.vividsolutions.jts.operation.distance.DistanceOp

import scala.collection.mutable

object Clause {
  def isVariable(input: String): Boolean = {
    Character.isUpperCase(input.head)
  }

  def isWildCard(input: String): Boolean = {
    input.head == '_'
  }
}

trait EntityContainer {
  def id: Data.EventId

  def entity: Seq[String]
}

trait Clause {
  def replaceLabel(target: String, newLabel: String): Clause
}

trait HeadClause extends Clause with EntityContainer {
  override def replaceLabel(target: String, newLabel: String): HeadClause
}

trait BodyClause extends Clause {
  override def replaceLabel(target: String, newLabel: String): BodyClause

  def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict]
}

case class UnionAll(input: Seq[String], result: String, strict: Boolean)
  extends BodyClause {
  override val toString = {
    s"Union_All ${if (strict) "!" else ""} [${input.mkString(", ")}] $result"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    if (strict) {
      dict
        .map { labels: Predicate.GroundingDict =>
          val intervalsDict: Map[String, Intervals] = labels._3
          val inputIntervals: Seq[Intervals] = input map { arg =>
            if (intervalsDict contains arg)
              intervalsDict(arg)
            else
              Intervals.fromString(arg)
          }
          val union = Intervals.union(inputIntervals)

          if (union.isEmpty)
            null
          else
            labels.copy(_3 = intervalsDict + (result -> union))
        }
        .filter(_ != null)

    } else {
      dict map { labels: Predicate.GroundingDict =>
        val intervalsDict: Map[String, Intervals] = labels._3
        val inputIntervals: Seq[Intervals] = input map { arg =>
          if (intervalsDict contains arg)
            intervalsDict(arg)
          else
            Intervals.fromString(arg)
        }
        val union = Intervals.union(inputIntervals)

        labels.copy(_3 = intervalsDict + (result -> union))
      }
    }
  }

  override def replaceLabel(target: String, newLabel: String): UnionAll = this
}

case class ComplementAll(input: Seq[String], result: String)
  extends BodyClause {
  override val toString = {
    s"Complement_All [${input.mkString(", ")}] $result"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    dict map { labels: Predicate.GroundingDict =>
      val intervalsDict: Map[String, Intervals] = labels._3
      val inputIntervals: Seq[Intervals] = input map { arg =>
        if (intervalsDict contains arg)
          intervalsDict(arg)
        else
          Intervals.fromString(arg)
      }
      val union = Intervals.union(inputIntervals)
      val complement = Intervals.complement(union)

      labels.copy(_3 = intervalsDict + (result -> complement))
    }
  }

  override def replaceLabel(target: String, newLabel: String): ComplementAll = this
}

case class IntersectAll(input: Seq[String], result: String, strict: Boolean)
  extends BodyClause {
  override val toString = {
    s"Intersect_All ${if (strict) "!" else ""} [${input.mkString(", ")}] $result"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    if (strict) {
      dict
        .map { labels: Predicate.GroundingDict =>
          val intervalsDict: Map[String, Intervals] = labels._3
          val inputIntervals: Seq[Intervals] = input map { arg =>
            if (intervalsDict contains arg)
              intervalsDict(arg)
            else
              Intervals.fromString(arg)
          }
          val intersection = Intervals.intersect(inputIntervals)

          if (intersection.isEmpty)
            null
          else
            labels.copy(_3 = intervalsDict + (result -> intersection))
        }
        .filter(_ != null)

    } else {
      dict map { labels: Predicate.GroundingDict =>
        val intervalsDict: Map[String, Intervals] = labels._3
        val inputIntervals: Seq[Intervals] = input map { arg =>
          if (intervalsDict contains arg)
            intervalsDict(arg)
          else
            Intervals.fromString(arg)
        }
        val intersection = Intervals.intersect(inputIntervals)

        labels.copy(_3 = intervalsDict + (result -> intersection))
      }
    }
  }

  override def replaceLabel(target: String, newLabel: String): IntersectAll = this
}

case class RelativeComplementAll(baseInput: String, excludedInput: Seq[String], result: String, strict: Boolean)
  extends BodyClause {
  override val toString = {
    s"Relative_Complement_All ${if (strict) "!" else ""} $baseInput [${excludedInput.mkString(", ")}] $result"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    if (strict) {
      dict
        .map { labels: Predicate.GroundingDict =>
          val intervalsDict: Map[String, Intervals] = labels._3
          val baseInputIntervals: Intervals = intervalsDict.getOrElse(baseInput, Intervals.fromString(baseInput))
          val excludedInputIntervals: Seq[Intervals] = excludedInput map { arg =>
            if (intervalsDict contains arg)
              intervalsDict(arg)
            else
              Intervals.fromString(arg)
          }
          val unionOfExcluded = Intervals.union(excludedInputIntervals)
          val complementOfExcluded = Intervals.complement(unionOfExcluded)
          val relativeComplement = complementOfExcluded & baseInputIntervals

          if (relativeComplement.isEmpty)
            null
          else
            labels.copy(_3 = intervalsDict + (result -> relativeComplement))
        }
        .filter(_ != null)

    } else {
      dict map { labels: Predicate.GroundingDict =>
        val intervalsDict: Map[String, Intervals] = labels._3
        val baseInputIntervals: Intervals = intervalsDict.getOrElse(baseInput, Intervals.fromString(baseInput))
        val excludedInputIntervals: Seq[Intervals] = excludedInput map { arg =>
          if (intervalsDict contains arg)
            intervalsDict(arg)
          else
            Intervals.fromString(arg)
        }
        val unionOfExcluded = Intervals.union(excludedInputIntervals)
        val complementOfExcluded = Intervals.complement(unionOfExcluded)
        val relativeComplement = complementOfExcluded & baseInputIntervals

        labels.copy(_3 = intervalsDict + (result -> relativeComplement))
      }
    }
  }

  override def replaceLabel(target: String, newLabel: String): RelativeComplementAll = this
}

/* ====================== Maritime Domain ====================== */

case class IsFishing(vesselId: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): IsFishing = this

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    dict
      .filter {
        case (entities, values, intervals, timePoints) =>
          val id = values(vesselId)
          ExtraLogicReasoning.getVesselTypes.get(id) match {
            case Some(v) => {
              v == "fishing"
            }
            case None => {
              false
            }
          }
      }
  }
}

case class IsAtTravelSpeed(vessel: String, speed: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): IsAtTravelSpeed = this

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    dict.filter{
      v =>
        val velocity = v._2(speed).toDouble
        val id = v._2(vessel)
        val (min,max,avg) = ExtraLogicReasoning.getVesselTypes.get(id) match {
          case Some(vType) => {
            ExtraLogicReasoning.getSpeedTypes(vType)
          }
          case None => {
            ExtraLogicReasoning.getSpeedTypes("other")
          }
        }
        velocity < max && velocity > min
    }
  }
}

case class NotAtTravelSpeed(vessel: String, speed: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): NotAtTravelSpeed = this

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    dict.filter{
      v =>
        val velocity = v._2(speed).toDouble
        val id = v._2(vessel)
        val (min,max,avg) = ExtraLogicReasoning.getVesselTypes.get(id) match {
          case Some(vType) => {
            ExtraLogicReasoning.getSpeedTypes(vType)
          }
          case None => {
            ExtraLogicReasoning.getSpeedTypes("other")
          }
        }
        velocity > max && velocity < min
    }
  }
}

case class NotNearPorts(lon: String, lat: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): NotNearPorts = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]
    dict.foreach {
      case (entities, values, intervals, timePoints) =>
        val longitude = values(lon)
        val latitude = values(lat)
        if (ExtraLogicReasoning.notClose(longitude.toDouble, latitude.toDouble)) {
          newDict = newDict ++ Iterable((entities, values, intervals, timePoints))
        }
    }
    newDict
  }
}

case class NearPorts(lon: String, lat: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): NearPorts = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]
    dict.foreach {
      case (entities, values, intervals, timePoints) =>
        val longitude = values(lon)
        val latitude = values(lat)
        if (!ExtraLogicReasoning.notClose(longitude.toDouble, latitude.toDouble)) newDict = newDict ++ Iterable((entities, values, intervals, timePoints))
    }
    newDict
  }
}

case class InArea(lon: String, lat: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): InArea = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]
    dict.foreach {
      case (entities, values, intervals, timePoints) =>
        val longitude = values(lon)
        val latitude = values(lat)
        if (ExtraLogicReasoning.isInArea(longitude.toDouble, latitude.toDouble)) newDict = newDict ++ Iterable((entities, values, intervals, timePoints))
    }
    newDict
  }
}

case class NotInPorts(entityId: String, lon: String, lat: String, timePoint: String, iInterval: String, finalInterval: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): NotInPorts = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    dict.map {
      case (entities, values, intervals, timePoints) =>
        val longitude = values(lon)
        val latitude = values(lat)

        // get the intervals for which we have coordinates
        val subIntervals = intervals(iInterval).t.filter(i => timePoints(timePoint).contains(i._1))
        var newIntervals = intervals

        // check if it is close at current time
        if (ExtraLogicReasoning.notClose(longitude.toDouble, latitude.toDouble)) {
          // add the interval to map
          newIntervals += (finalInterval -> Intervals(subIntervals))
        }
        else {
          // else add empty interval
          newIntervals += (finalInterval -> Intervals.empty)
        }
        (entities, values, newIntervals, timePoints)
    }(collection.breakOut)
  }
}

case class DistanceLessThan(lon1: String, lat1: String, lon2: String, lat2: String, threshold: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): DistanceLessThan = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]
    dict.foreach {
      case (entities, values, ints, points) =>
        val longitude1 = values(lon1)
        val latitude1 = values(lat1)
        val longitude2 = values(lon2)
        val latitude2 = values(lat2)
        if (SpatialReasoning.getHarvesineDistance(longitude1.toDouble, latitude1.toDouble, longitude2.toDouble, latitude2.toDouble) < threshold.toDouble) {
          newDict = newDict ++ Iterable((entities, values, ints, points))
        }
    }
    newDict
  }
}

case class ThresholdGreater(speed: String, threshold: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): ThresholdGreater = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    dict.filter(_._2(speed).toDouble >= threshold.toDouble)
  }
}

case class ThresholdLess(speed: String, threshold: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): ThresholdLess = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    dict.filter(_._2(speed).toDouble < threshold.toDouble)
  }
}

case class InAreaSpeedGreater(areaName: String, speed: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): InAreaSpeedGreater = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    dict.filter(d => d._2(speed).toDouble > ExtraLogicReasoning.getSpeedArea(d._2(areaName)))
  }
}

case class InAreaSpeedLess(areaName: String, speed: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): InAreaSpeedLess = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    dict.filter(d => d._2(speed).toDouble <= ExtraLogicReasoning.getSpeedArea(d._2(areaName)))
  }
}

case class IntDurGreater(inInterval: String, duration: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): IntDurGreater = this

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    dict
      .map {
        case (entities, values, intervals, timePoints) =>
          if (intervals(inInterval).isEmpty) {
            var newIntervals = intervals
            newIntervals += (inInterval -> Intervals.empty)
            (entities, values, newIntervals, timePoints)
          }
          else {
            val filteredIntervals = intervals(inInterval).t.filter { i =>
              (i._2 - i._1) >= duration.toDouble
            }
            var newIntervals = intervals
            newIntervals += (inInterval -> Intervals(filteredIntervals))
            (entities, values, newIntervals, timePoints)
          }
      }
  }
}

/* ====================== Maritime Domain ====================== */

/* ====================== Aviation Domain ====================== */

case class CloseToModelCruise(aircraftId: String, altitude: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): CloseToModelCruise = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Long]])] = {
    dict.filter(d => d._2(altitude).toDouble > (ExtraLogicReasoning.getFlightLevel(d._2(aircraftId)) - 5000))
  }
}

case class DecideTopOfClimb(aircraftId: String, altitude:String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): DecideTopOfClimb = this

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {

    ExtraLogicReasoning.getTopOfC.filter(d => d._2(altitude).toDouble >
      (ExtraLogicReasoning.getFlightLevel(d._2(aircraftId)) - 5000))

  }
}

case class DecideTopOfDescent(aircraftId: String, altitude:String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): DecideTopOfDescent = this

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {

    ExtraLogicReasoning.getTopOfD.filter(d => d._2(altitude).toDouble >
      (ExtraLogicReasoning.getFlightLevel(d._2(aircraftId)) - 5000))

  }
}

case class CalcAngles(aircraftId: String, altitude:String, longitude:String, latitude:String, speed: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): CalcAngles = this

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {

    val trip = dict.groupBy(_._1.head).filterNot(v => ExtraLogicReasoning.getRecTopOfDescent(v._1))
      .mapValues(_.toSeq.sortBy(_._4.values.head.head)
        .map(v=>(v._2(longitude),v._2(latitude),v._2(altitude),v._2(speed),v._4.values.head.head)).sliding(5).toList)

    //println(trip)

    /*ExtraLogicReasoning.constructTopOfCD(trip.map(v=>(v._1,ExtraLogicReasoning.computeAngle3d(v._1,v._2))).filter(_._2.nonEmpty),
      dict, aircraftId,altitude)*/

    ExtraLogicReasoning.constructTopOfCD(trip.map(v=>(v._1,ExtraLogicReasoning.computeAngle2d(v._1,v._2))).filter(_._2.nonEmpty),
      dict, aircraftId, altitude)

  }
}

case class AwayFromRoute(aircraftId: String, longitude:String, latitude:String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): AwayFromRoute = this

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {

    dict.filter { d =>

      val ptc = SpatialReasoning.toNvector(new Coordinate(d._2(longitude).toDouble, d._2(latitude).toDouble))

      val singlePoint: Point = new GeometryFactory().createPoint(ptc)

      val plan_traj = ExtraLogicReasoning.getFlightPlanTrajectory(d._2(aircraftId))

      if (!plan_traj.isEmpty) {

        if (DistanceOp.isWithinDistance(singlePoint, plan_traj, 5.0 / 6373)) false else true
      } else false
    }
  }
}

case class NotAwayFromRoute(aircraftId: String, longitude:String, latitude:String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): NotAwayFromRoute = this

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {

    dict.filter { d =>

      val ptc = SpatialReasoning.toNvector(new Coordinate(d._2(longitude).toDouble, d._2(latitude).toDouble))

      val singlePoint: Point = new GeometryFactory().createPoint(ptc)

      val plan_traj = ExtraLogicReasoning.getFlightPlanTrajectory(d._2(aircraftId))

      if (!plan_traj.isEmpty) {

        if (DistanceOp.isWithinDistance(singlePoint, plan_traj, 5.0 / 6373)) true else false
      } else false
    }
  }
}

/* ====================== Aviation Domain ====================== */

// happensAt
case class HappensAtIE(id: InstantEventId, entity: Seq[String], time: String)
  extends HeadClause
    with BodyClause
    with EntityContainer {

  override val toString = {
    s"HappensAt [${id.name} ${entity.mkString(" ")}] $time"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    var q = new mutable.Queue[Predicate.GroundingDict]
    dict.foreach{
      labels =>
        val entityDict = labels._2
        val timePointsDict = labels._4
        val groundedEntity = entity.map(arg => entityDict.getOrElse(arg, arg))

        var response: Map[Seq[String], Set[Long]] = data.getIETime(id, groundedEntity)

        if (timePointsDict contains time) {
          // Compare with grounded values
          val gtime = timePointsDict(time)
          response = response
            .mapValues {
              gtime & _
            }
            .filter(_._2.nonEmpty)
        }

        val tempResponse = response.map { case (e, t) =>
          val additions = groundedEntity
            .zip(e)
            .filter(x => Clause.isVariable(x._1))

          labels.copy(_2 = entityDict ++ additions, _4 = timePointsDict + (time -> t))
        }(collection.breakOut): Vector[Predicate.GroundingDict]

        tempResponse.foreach(q += _)
    }
    q
  }

  override def replaceLabel(target: String, withLabel: String): HappensAtIE = {
    val argsIndex = entity indexOf target
    val newEntity =
      if (argsIndex != -1)
        entity updated(argsIndex, withLabel)
      else
        entity

    val newTime =
      if (time == target)
        withLabel
      else
        time

    HappensAtIE(id, newEntity, newTime)
  }
}

// initiatedAt
case class InitiatedAt(id: FluentId, entity: Seq[String], time: String)
  extends HeadClause
    with EntityContainer {

  override val toString = {
    s"InitiatedAt [${id.name} ${entity.mkString(" ")} = ${id.value}] $time"
  }

  override def replaceLabel(target: String, withLabel: String): InitiatedAt = {
    val newId =
      if (id.value == target)
        FluentId(id.name, id.numOfArgs, withLabel)
      else
        id

    val argsIndex = entity indexOf target
    val newEntity =
      if (argsIndex != -1)
        entity updated(argsIndex, withLabel)
      else
        entity

    val newTime =
      if (time == target)
        withLabel
      else
        time

    InitiatedAt(newId, newEntity, newTime)
  }
}

// terminatedAt
case class TerminatedAt(id: FluentId, entity: Seq[String], time: String)
  extends HeadClause
    with EntityContainer {

  override val toString = {
    s"TerminatedAt [${id.name} ${entity.mkString(" ")} = ${id.value}] $time"
  }

  override def replaceLabel(target: String, withLabel: String): TerminatedAt = {
    val newId =
      if (id.value == target)
        FluentId(id.name, id.numOfArgs, withLabel)
      else
        id

    val argsIndex = entity indexOf target
    val newEntity =
      if (argsIndex != -1)
        entity updated(argsIndex, withLabel)
      else
        entity

    val newTime =
      if (time == target)
        withLabel
      else
        time

    TerminatedAt(newId, newEntity, newTime)
  }
}

// holdsFor
case class HoldsFor(id: FluentId, entity: Seq[String], time: String, strict: Boolean)
  extends HeadClause
    with BodyClause
    with EntityContainer {

  override val toString = {
    s"HoldsFor ${if (strict) "!" else ""} [${id.name} ${entity.mkString(" ")} = ${id.value}] $time"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    var q = new mutable.Queue[Predicate.GroundingDict]
    if (strict) {
      dict.foreach{
        labels =>
          val entityDict = labels._2
          val intervalsDict = labels._3
          val groundedEntity = entity.map(arg => entityDict.getOrElse(arg, arg))

          val response = data.getFluentTime(id, groundedEntity)

          val tempResponse = response.collect { case (e, t) if t.nonEmpty =>
            val additions = groundedEntity
              .zip(e)
              .filter(x => Clause.isVariable(x._1))

            labels.copy(_2 = entityDict ++ additions, _3 = intervalsDict + (time -> t))
          }(collection.breakOut): Vector[Predicate.GroundingDict]

          tempResponse.foreach(q += _)
      }
    }
    else {
      dict.foreach{
        labels =>
          val entityDict = labels._2
          val intervalsDict = labels._3
          val groundedEntity = entity.map(arg => entityDict.getOrElse(arg, arg))

          val response = data.getFluentTime(id, groundedEntity)

          val tempResponse = response.map { case (e, t) =>
            val additions = groundedEntity
              .zip(e)
              .filter(x => Clause.isVariable(x._1))

            labels.copy(_2 = entityDict ++ additions, _3 = intervalsDict + (time -> t))
          }(collection.breakOut): Vector[Predicate.GroundingDict]

          tempResponse.foreach(q += _)
      }
    }
    q
  }

  override def replaceLabel(target: String, withLabel: String): HoldsFor = {
    val newId =
      if (id.value == target)
        FluentId(id.name, id.numOfArgs, withLabel)
      else
        id

    val argsIndex = entity indexOf target
    val newEntity =
      if (argsIndex != -1)
        entity updated(argsIndex, withLabel)
      else
        entity

    val newTime =
      if (time == target)
        withLabel
      else
        time

    HoldsFor(newId, newEntity, newTime, strict)
  }
}

case class HoldsAt(id: FluentId, entity: Seq[String], time: String)
  extends BodyClause
    with EntityContainer {

  override val toString = {
    s"HoldsAt [${id.name} ${entity.mkString(" ")} = ${id.value}] $time"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    var q = new mutable.Queue[Predicate.GroundingDict]
    dict.foreach{
      labels =>
        val entityDict = labels._2
        val timePointsDict = labels._4
        val groundedEntity = entity.map(arg => entityDict.getOrElse(arg, arg))

        val response: Map[Seq[String], Intervals] = data.getFluentTime(id, groundedEntity)
        val gtime = timePointsDict(time)
        val tempResponse = response
          .mapValues(gtime filter _.contains)
          .collect { case (e, t) if t.nonEmpty =>
            val additions = groundedEntity
              .zip(e)
              .filter(x => Clause.isVariable(x._1))

            labels.copy(_2 = entityDict ++ additions, _4 = timePointsDict + (time -> t))
          }(collection.breakOut): Vector[Predicate.GroundingDict]

        tempResponse.foreach(q += _)
    }
    q
  }

  override def replaceLabel(target: String, withLabel: String): HoldsAt = {
    val newId =
      if (id.value == target)
        FluentId(id.name, id.numOfArgs, withLabel)
      else
        id

    val argsIndex = entity indexOf target
    val newEntity =
      if (argsIndex != -1)
        entity updated(argsIndex, withLabel)
      else
        entity

    val newTime =
      if (time == target)
        withLabel
      else
        time

    HoldsAt(newId, newEntity, newTime)
  }

}

case class HappensAtFluentStart(id: FluentId, entity: Seq[String], time: String)
  extends BodyClause
    with EntityContainer {

  override val toString = {
    s"HappensAt Start [${id.name} ${entity.mkString(" ")} = ${id.value}] $time"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    var q = new mutable.Queue[Predicate.GroundingDict]
    dict.foreach{
      labels =>
        val entityDict = labels._2
        val timePointsDict = labels._4
        val groundedEntity = entity.map(arg => entityDict.getOrElse(arg, arg))

        val response: Map[Seq[String], Intervals] = data.getFluentTime(id, groundedEntity)
        val results =
          if (timePointsDict contains time) {
            val gtime = timePointsDict(time)
            response mapValues (x => gtime & x.startPoints)
          } else
            response mapValues (_.startPoints)

        val tempResponse = results.collect { case (e, t) if t.nonEmpty =>
          val additions = groundedEntity
            .zip(e)
            .filter(x => Clause.isVariable(x._1))

          labels.copy(_2 = entityDict ++ additions, _4 = timePointsDict + (time -> t))
        }(collection.breakOut): Vector[Predicate.GroundingDict]

        tempResponse.foreach(q += _)
    }
    q
  }

  override def replaceLabel(target: String, withLabel: String): HappensAtFluentStart = {
    val newId =
      if (id.value == target)
        FluentId(id.name, id.numOfArgs, withLabel)
      else
        id

    val argsIndex = entity indexOf target
    val newEntity =
      if (argsIndex != -1)
        entity updated(argsIndex, withLabel)
      else
        entity

    val newTime =
      if (time == target)
        withLabel
      else
        time

    HappensAtFluentStart(newId, newEntity, newTime)
  }
}

case class HappensAtFluentEnd(id: FluentId, entity: Seq[String], time: String)
  extends BodyClause
    with EntityContainer {

  override val toString = {
    s"HappensAt End [${id.name} ${entity.mkString(" ")} = ${id.value}] $time"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    var q = new mutable.Queue[Predicate.GroundingDict]
    dict.foreach{
      labels =>
        val entityDict = labels._2
        val timePointsDict = labels._4
        val groundedEntity = entity.map(arg => entityDict.getOrElse(arg, arg))

        val response: Map[Seq[String], Intervals] = data.getFluentTime(id, groundedEntity)
        val results =
          if (timePointsDict contains time) {
            val gtime = timePointsDict(time)
            response mapValues (x => gtime & x.endPoints)
          } else
            response mapValues (_.endPoints)

        val tempResponse = results.collect { case (e, t) if t.nonEmpty =>
          val additions = groundedEntity
            .zip(e)
            .filter(x => Clause.isVariable(x._1))

          labels.copy(_2 = entityDict ++ additions, _4 = timePointsDict + (time -> t))
        }(collection.breakOut): Vector[Predicate.GroundingDict]

        tempResponse.foreach(q += _)
    }
    q
  }

  override def replaceLabel(target: String, withLabel: String): HappensAtFluentEnd = {
    val newId =
      if (id.value == target)
        FluentId(id.name, id.numOfArgs, withLabel)
      else
        id

    val argsIndex = entity indexOf target
    val newEntity =
      if (argsIndex != -1)
        entity updated(argsIndex, withLabel)
      else
        entity

    val newTime =
      if (time == target)
        withLabel
      else
        time

    HappensAtFluentEnd(newId, newEntity, newTime)
  }
}

case class NotHappensAtIE(id: InstantEventId, entity: Seq[String], time: String)
  extends BodyClause
    with EntityContainer {

  override val toString = {
    s"Not HappensAt [${id.name} ${entity.mkString(" ")}] $time"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    var q = new mutable.Queue[Predicate.GroundingDict]
    dict.foreach{
      labels =>
        val entityDict = labels._2
        val timePointsDict = labels._4
        val groundedEntity = entity.map(arg => entityDict.getOrElse(arg, arg))

        val response: Map[Seq[String], Set[Long]] = data.getIETime(id, groundedEntity)
        val gtime = timePointsDict(time)
        val tempResponse = response
          .mapValues(gtime -- _)
          .collect { case (e, t) if t.nonEmpty =>
            val additions = groundedEntity
              .zip(e)
              .filter(x => Clause.isVariable(x._1))

            labels.copy(_2 = entityDict ++ additions, _4 = timePointsDict + (time -> t))
          }(collection.breakOut): Vector[Predicate.GroundingDict]

        tempResponse.foreach(q += _)
    }
    q
  }

  override def replaceLabel(target: String, withLabel: String): NotHappensAtIE = {
    val argsIndex = entity indexOf target
    val newEntity =
      if (argsIndex != -1)
        entity updated(argsIndex, withLabel)
      else
        entity

    val newTime =
      if (time == target)
        withLabel
      else
        time

    NotHappensAtIE(id, newEntity, newTime)
  }
}

case class NotHoldsAt(id: FluentId, entity: Seq[String], time: String)
  extends BodyClause
    with EntityContainer {

  override val toString = {
    s"Not HoldsAt [${id.name} ${entity.mkString(" ")} = ${id.value}] $time"
  }

  override def resolve(data: Execute.EventDB, dict: Iterable[Predicate.GroundingDict]): Iterable[Predicate.GroundingDict] = {
    var q = new mutable.Queue[Predicate.GroundingDict]
    dict.foreach{
      labels =>
        val entityDict = labels._2
        val timePointsDict = labels._4
        val groundedEntity = entity.map(arg => entityDict.getOrElse(arg, arg))

        val response: Map[Seq[String], Intervals] = data.getFluentTime(id, groundedEntity)

        val gtime = timePointsDict(time)
        val tempResponse = response
          .mapValues(gtime filterNot _.contains)
          .collect { case (e, t) if t.nonEmpty =>
            val additions = groundedEntity
              .zip(e)
              .filter(x => Clause.isVariable(x._1))

            labels.copy(_2 = entityDict ++ additions, _4 = timePointsDict + (time -> t))
          }(collection.breakOut): Vector[Predicate.GroundingDict]

        tempResponse.foreach(q += _)
    }
    q
  }

  override def replaceLabel(target: String, withLabel: String): NotHoldsAt = {
    val newId =
      if (id.value == target)
        FluentId(id.name, id.numOfArgs, withLabel)
      else
        id

    val argsIndex = entity indexOf target
    val newEntity =
      if (argsIndex != -1)
        entity updated(argsIndex, withLabel)
      else
        entity

    val newTime =
      if (time == target)
        withLabel
      else
        time

    NotHoldsAt(newId, newEntity, newTime)
  }

}

