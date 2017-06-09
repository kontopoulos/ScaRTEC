package RTEC.Data

import RTEC.Execute.EventDB
import RTEC._

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

case class RemoveDuplicates(id1: String, id2: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): RemoveDuplicates = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    dict.filter {
      x =>
        x._1.head != x._1(1)
    }
  }
}

/* ====================== Maritime Domain ====================== */

case class NotNearPorts(lon: String, lat: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): NotNearPorts = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]
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

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]
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

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]
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

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
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

case class ExtendedDelays(vessel: String, inInterval: String, duration: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): ExtendedDelays = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    dict.map {
      case (entities, values, intervals, timePoints) =>
        if (intervals(inInterval).isEmpty) {
          var newIntervals = intervals
          newIntervals += (duration -> Intervals.empty)
          (entities, values, newIntervals, timePoints)
        }
        else {
          // get intervals whose speed is below five miles
          val filteredIntervals = intervals(inInterval).t.filter {
            case (startPoint, endPoint) =>
              var speed = 0.0
              // if it never ends that means it has a very low speed
              if (endPoint != -1) {
                try {
                  // get coordinates of start point
                  val (lonS, latS) = data.getInstantEventCoordinates(InstantEventId("coord", 3), entities.head, startPoint - 1)
                  // get coordinates of end point
                  val (lonE, latE) = data.getInstantEventCoordinates(InstantEventId("coord", 3), entities.head, endPoint - 1)
                  val spatialDistance = SpatialReasoning.getHarvesineDistance(lonS.toDouble, latS.toDouble, lonE.toDouble, latE.toDouble)
                  // temporal distance in hours
                  val temporalDistance = (endPoint - startPoint).toDouble / 3600
                  // calculate speed in miles per hour
                  speed = spatialDistance / temporalDistance
                }
                catch {
                  case e: Exception => speed = 6.0
                }
              }
              // speed must be below 5.0 miles per hour
              speed < 5.0
          }
          var newIntervals = intervals
          newIntervals += (duration -> Intervals(filteredIntervals))
          (entities, values, newIntervals, timePoints)
        }
    }(collection.breakOut)
  }
}

case class DiffBetween(t2: String, t1: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): DiffBetween = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    dict.map {
      case (entities, values, intervals, timePoints) =>

        val first = timePoints(t1)
        val second = timePoints(t2)

        val filtered = first.zip(second).filter {
          case (a, b) =>
            val diff = Math.abs(a - b)
            diff > 0 && diff < 3600
        }

        val newFirst = filtered.map(_._1)
        val newSecond = filtered.map(_._2)

        var newPoints = timePoints
        newPoints += (t1 -> newFirst)
        newPoints += (t2 -> newSecond)

        (entities, values, intervals, newPoints)
    }(collection.breakOut)
  }
}

case class DistanceLessThan(lon1: String, lat1: String, lon2: String, lat2: String, threshold: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): DistanceLessThan = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]
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

case class ManyStoppedVessels(vessel: String, cellId: String, point: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): ManyStoppedVessels = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]
    dict.foreach {
      case (entities, values, ints, points) =>
        // get current cell
        val currentCell = values(cellId)
        // get all nearby cells
        val nearbyCells = ExtraLogicReasoning.getNearbyCellsByCell(currentCell)
        // create the time points
        val searchIntervals = points(point)

        // get stoppedNIP vessel ids inside the search interval
        val stoppedVesselsInTime = data.getFluent(FluentId("stoppedNIP", 1, "true"))
          // map to vessel id and intervals
          .map(x => (x._1.head, x._2))
          // take the vessels inside the search intervals
          .filter {
          case (vesselId, ints) =>
            val newInts = ints.t.map(_._1).filter {
              i =>
                // create search interval
                val start = i - 60
                val end = i + 60
                searchIntervals.exists(p => p >= start && p <= end)
            }
            newInts.nonEmpty
        } // map to vessel ids only
          .map(_._1).toSet

        val nearbyVessels = data.getInstantEvent(InstantEventId("coord", 3))
          // take only the coordinates for stopped in-time vessels
          .filter(x => stoppedVesselsInTime.contains(x._1.head))
          .map {
            x =>
              (x._1(1), x._1.last)
          }.filter {
          case (lon, lat) =>
            val enclosingCell = SpatialReasoning.getEnclosingGridCell(lon.toDouble, lat.toDouble)
            // enclosing grid cell must be one of the nearby cells
            nearbyCells.contains(enclosingCell)
        }
        if (nearbyVessels.size > 3) {
          // suspicious is an area if more than 3 stopped vessels are nearby
          newDict = newDict ++ Iterable((entities, values, ints, points))
        }
    }
    newDict
  }
}

case class ThresholdGreater(speed: String, threshold: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): ThresholdGreater = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    dict.filter(_._2(speed).toDouble >= threshold.toDouble)
  }
}

case class ThresholdLess(speed: String, threshold: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): ThresholdLess = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    dict.filter(_._2(speed).toDouble < threshold.toDouble)
  }
}

case class InAreaSpeedGreater(areaName: String, speed: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): InAreaSpeedGreater = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    dict.filter(d => d._2(speed).toDouble > ExtraLogicReasoning.getSpeedArea(d._2(areaName)))
  }
}

case class InAreaSpeedLess(areaName: String, speed: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): InAreaSpeedLess = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
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

case class CheckHeadingToVessels(vessel: String, lon: String, lat: String, heading: String, t: String) extends BodyClause {
  override def replaceLabel(target: String, newLabel: String): CheckHeadingToVessels = this

  override def resolve(data: EventDB, dict: Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]): Iterable[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])] = {
    var newDict = Iterable.empty[(Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])]
    dict.foreach {
      case (entities, values, ints, points) =>
        val vesselId = values(vessel)
        val longitude = values(lon).toDouble
        val latitude = values(lat).toDouble
        val headingOfVessel = values(heading).toDouble
        // get enclosing cell
        val enclosingCell = SpatialReasoning.getEnclosingGridCell(longitude, latitude)
        // get nearby cells
        val nearbyCells = ExtraLogicReasoning.getNearbyCellsByEnclosingCell(enclosingCell)
        // get nearby vessels at current time
        val nearbyVessels = data.getInstantEvent(InstantEventId("coord", 3)).filter(_._2.intersect(points(t)).nonEmpty).map(_._1).filter {
          ent =>
            val lon = ent(1).toDouble
            val lat = ent.last.toDouble
            // get current vessel enclosing cell
            val currentCell = SpatialReasoning.getEnclosingGridCell(lon, lat)
            // check if it is nearby
            nearbyCells.contains(currentCell)
        }.filterNot(_.head == vesselId)
        // true if it is headed fast towards another vessel (any nearby vessel)
        if (nearbyVessels.exists {
          x =>
            val longitude2 = x(1).toDouble
            val latitude2 = x.last.toDouble
            SpatialReasoning.hasHeading(longitude, latitude, headingOfVessel, longitude2, latitude2)
        }) {
          newDict = newDict ++ Iterable((entities, values, ints, points))
        }
    }
    newDict
  }
}

/* ====================== Maritime Domain ====================== */

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

        var response: Map[Seq[String], Set[Int]] = data.getIETime(id, groundedEntity)

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

        val response: Map[Seq[String], Set[Int]] = data.getIETime(id, groundedEntity)
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

