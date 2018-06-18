package RTEC.Execute

import RTEC._

class EventDB(val iEs: Map[Data.InstantEventId, Data.IEType],
              val fluents: Map[Data.FluentId, Data.FluentType]) extends Serializable {

  private var _iETime: Map[Data.InstantEventId, Map[Seq[String], Set[Long]]] = iEs.keys
    .map { ie =>
      ie -> Map[Seq[String], Set[Long]]()
    }(collection.breakOut)

  private var _fluentTime: Map[Data.FluentId, Map[Seq[String], Data.Intervals]] = fluents.keys
    .map { fluent =>
      fluent -> Map[Seq[String], Data.Intervals]()
    }(collection.breakOut)

  var _toDiscard: Map[Data.FluentId, Set[Seq[String]]] = Map()
  private var f: Map[Data.FluentId, Map[Seq[String], Data.Intervals]] = Map()

  def output: String = {
    val IEStr = {
      for {
        ie <- _iETime if iEs(ie._1) == Data.OutputIE
        data <- ie._2.mapValues(_.toVector.sorted)
      }
        yield s"${ie._1.name}(${data._1.mkString(",")}),[${data._2.mkString(",")}]"

    }.mkString("\n")

    val fluentStr = {
      for {
        fluent <- _fluentTime if fluents(fluent._1) == Data.SimpleFluent || fluents(fluent._1) == Data.OutputSDFluent
        data <- fluent._2.filter(_._2.nonEmpty)
      }
        yield s"${fluent._1.name}(${data._1.mkString(",")})=${fluent._1.value},[${data._2}]"

    }.mkString("\n")

    IEStr + "\n" + fluentStr
  }


  /**
    * Convert CE results to json
    * @return string of json
    */
  def toJson: String = {
    val fluentStr = {
      for {
        fluent <- _fluentTime if fluents(fluent._1) == Data.SimpleFluent || fluents(fluent._1) == Data.OutputSDFluent
        data <- fluent._2.filter(_._2.nonEmpty)
      }
        yield {
          data._2.t.filterNot(_._2 == -1).map{
            case (s,e) =>
              if (fluent._1.name == "lowSpeed" || fluent._1.name == "stopped" || fluent._1.name == "sailing" || fluent._1.name == "loitering") {
                "{"+"\"name\":\""+fluent._1.name+"\",\"vesselId\":\""+data._1.head+"\",\"startTime\":\""+s+"\",\"endTime\":\""+e+"\"}"
              }
              else if (fluent._1.name == "withinArea" || fluent._1.name == "highSpeedIn") {
                "{"+"\"name\":\""+fluent._1.name+"\",\"vesselId\":\""+data._1.head+"\",\"areaId\":\""+data._1(1)+"\",\"startTime\":\""+s+"\",\"endTime\":\""+e+"\"}"
              }
              else if (fluent._1.name == "rendezVouz") {
                "{"+"\"name\":\""+fluent._1.name+"\",\"vesselId1\":\""+data._1.head+"\",\"vesselId2\":\""+data._1(1)+"\",\"startTime\":\""+s+"\",\"endTime\":\""+e+"\"}"
              }
          }.mkString(",")
        }
    }
    // build json
    val json = "[" + fluentStr.filterNot(e => e.isEmpty || e.contains("other_time")).mkString(",")
    json.substring(0, json.length) + "]\n"
  }

  /**
    * Gets the number of produced output events
    * @return numEvents
    */
  def getNumCEs: Int = {
    val numIEs = {
      for {
        ie <- _iETime if iEs(ie._1) == Data.OutputIE
        data <- ie._2.mapValues(_.toVector.sorted)
      }
        yield data._2
    }.flatten.size

    val numFluents = {
      for {
        fluent <- _fluentTime if fluents(fluent._1) == Data.SimpleFluent || fluents(fluent._1) == Data.OutputSDFluent
        data <- fluent._2.filter(_._2.nonEmpty)
      }
        yield data._2.t
    }.flatten.size

    numIEs + numFluents
  }

  /**
    * Gets non terminated fluents of database
    * @return fluents
    */
  def getNonTerminatedFluents: Map[Data.FluentId, Map[Seq[String], Data.Intervals]] = {
    _fluentTime.map {
      case (fl, argsAndIntervals) =>
        val newArgsAndIntervals = argsAndIntervals
          .map(x => (x._1, Data.Intervals(x._2.t.filter(_._2 == -1))))
          .filter {
            _._2.t.nonEmpty
          }
        (fl, newArgsAndIntervals)
    }(collection.breakOut)
  }

  /**
    * Updates intervals from previous window
    * @param prevFluents
    */
  def amalgamate(prevFluents: Map[Data.FluentId, Map[Seq[String], Data.Intervals]]): Unit = {
    f = prevFluents
  }

  def getEntities(id: Data.EventId, pattern: Seq[String] = null): Iterable[Seq[String]] = {
    id match {
      case iEId: Data.InstantEventId =>
        if (pattern != null) {
          val allEntities: Iterable[Seq[String]] = _iETime(iEId).keys
          allEntities.filter {
            e =>
              e.zipWithIndex.forall { pair =>
                Data.Clause.isWildCard(pair._1) || pair._1 == pattern(pair._2)
              }
          }
        } else
          _iETime(iEId).keys

      case fluentId: Data.FluentId =>
        if (pattern != null) {
          val allEntities: Iterable[Seq[String]] = _fluentTime(fluentId).keys
          allEntities.filter {
            e =>
              e.zipWithIndex.forall { pair =>
                val cmp = pattern(pair._2)
                Data.Clause.isWildCard(cmp) || pair._1 == cmp
              }
          }
        } else
          _fluentTime(fluentId).keys

    }
  }

  def getIETime(id: Data.InstantEventId, entity: Seq[String]): Map[Seq[String], Set[Long]] = {
    if (entity exists (x => Data.Clause.isWildCard(x) || Data.Clause.isVariable(x))) {
      val allEntities: Map[Seq[String], Set[Long]] = _iETime(id)
      allEntities.filter {
        e =>
          val temp = e._1.zip(entity)
          temp.forall { pair =>
            pair._1 == pair._2 || Data.Clause.isWildCard(pair._2) || Data.Clause.isVariable(pair._2)
          }
      }
    } else {
      _iETime(id) get entity match {
        case Some(t) =>
          Map(entity -> t)

        case None =>
          Map(entity -> Set.empty[Long])
      }
    }
  }

  def getFluentTime(id: Data.FluentId, entity: Seq[String]): Map[Seq[String], Data.Intervals] = {
    if (entity exists (x => Data.Clause.isWildCard(x) || Data.Clause.isVariable(x))) {
      val allEntities: Map[Seq[String], Data.Intervals] = _fluentTime(id)
      allEntities.filter {
        e =>
          val temp = e._1.zip(entity)
          temp.forall { pair =>
            pair._1 == pair._2 || Data.Clause.isWildCard(pair._2) || Data.Clause.isVariable(pair._2)
          }
      }
    } else
      Map(entity -> _fluentTime(id).getOrElse(entity, Data.Intervals.empty))
  }

  def updateIE(input: Iterable[((Data.InstantEventId, Seq[String]), Set[Long])]): Unit = {
    input foreach { ie =>
      val m: Map[Seq[String], Set[Long]] = _iETime(ie._1._1)
      val oldTime: Set[Long] = m.getOrElse(ie._1._2, Set())
      _iETime += (ie._1._1 -> (m + (ie._1._2 -> (oldTime ++ ie._2))))
    }
  }

  def updateFluent(input: Iterable[((Data.FluentId, Seq[String]), Data.Intervals)], flag: Boolean): Unit = {
    input foreach { fluent =>
      val m: Map[Seq[String], Data.Intervals] = _fluentTime(fluent._1._1)
      val oldIntervals = m.getOrElse(fluent._1._2, Data.Intervals.empty)
      _fluentTime += (fluent._1._1 -> (m + (fluent._1._2 -> (oldIntervals | fluent._2))))
    }
    if (flag) {
      if (input.nonEmpty) {
        val currentFluent = input.head._1._1
        val (current, left) = f.partition(_._1 == currentFluent)
        f = left
        concatenateIntervals(current)
      }
    }
  }

  /**
    * Concatenates intervals with current window
    * @param prevFluents
    */
  def concatenateIntervals(prevFluents: Map[Data.FluentId, Map[Seq[String], Data.Intervals]]): Unit = {
    var oldFluents: Map[Data.FluentId, Map[Seq[String], Data.Intervals]] = Map()
    // discard any entities that may need deletion
    prevFluents.foreach{
      case (fluent,ents) =>
        if (_toDiscard.contains(fluent)) {
          val newEnts = ents.filterNot(x => _toDiscard(fluent).contains(x._1))
          oldFluents += (fluent -> newEnts)
        }
        else {
          oldFluents += (fluent -> ents)
        }
    }
    // concatenate intervals
    oldFluents.foreach {
      case (f, argsAndIntervals) =>
        if (_fluentTime.contains(f)) {
          var otherArgsAndInts = _fluentTime(f)
          argsAndIntervals.foreach {
            case (args, ints) =>
              if (otherArgsAndInts.contains(args)) {
                // get intervals from already stored fluents
                val otherInts = otherArgsAndInts(args)
                val start = ints.t
                val end = otherInts.t
                if (start.nonEmpty && end.nonEmpty) {
                  // concatenate intervals from previous window/s
                  val newInterval = (start.head._1, end.head._2)
                  // create final intervals
                  val newIntervals = Data.Intervals(newInterval +: otherInts.t.drop(1))
                  // store the new intervals
                  otherArgsAndInts += (args -> newIntervals)
                  // update event database
                  _fluentTime += (f -> otherArgsAndInts)
                }
              }
              else {
                otherArgsAndInts += (args -> ints)
                _fluentTime += (f -> otherArgsAndInts)
              }
          }
        }
    }
  }

  /* ==================== Maritime Domain ==================== */

  /**
    * Returns the coordinates at the current timePoint
    * @param id     instant event
    * @param entity vessel id
    * @param timePoint
    * @return coordinates
    */
  def getInstantEventCoordinates(id: Data.InstantEventId, entity: String, timePoint: Long): (String, String) = {
    val coordinates = _iETime(id)
      .filter(el => el._1.contains(entity) && el._2.contains(timePoint))
    val coords = coordinates.head._1.drop(1)
    val longitude = coords.head
    val latitude = coords.last
    (longitude, latitude)
  }

  /**
    * Gets fluent based on id
    * @param id fluent
    * @return
    */
  def getFluent(id: Data.FluentId): Map[Seq[String], Data.Intervals] = {
    _fluentTime(id)
  }

  /**
    * Gets instant event based on id
    * @param id event
    * @return
    */
  def getInstantEvent(id: Data.InstantEventId): Map[Seq[String], Set[Long]] = {
    _iETime(id)
  }
}
