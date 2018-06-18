package RTEC.Execute

import java.util.Properties

import RTEC.Data.HappensAtIE
import RTEC._
import integrated.{ComplexEvent, ComplexEventProducerInterceptor, ComplexEvent_KryoSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

class Reasoner {

  // static parameters
  private var _iEs: Set[Data.InstantEvent] = _
  private var _fluents: Set[Data.Fluent] = _
  private var _cachingOrder: Seq[(Data.EventId, String)] = _
  private var _iEPredicates: Seq[Data.IEPredicate] = _
  private var _fPredicates: Seq[Data.FPredicate] = _
  private var _inputEntities: Set[Data.InputEntity] = _
  private var _builtEntities: Seq[Data.BuiltEntity] = _
  private var _clock: Long = _
  private var _start: Long = _
  private var _end: Long = _

  // dynamic parameters
  private var _windowDB: EventDB = _
  private var _simpleFluentsNames: Set[String] = Set()
  private var _entities: Map[String, Iterable[Seq[String]]] = _
  private var _previousWindowEntities: Map[String, Iterable[Seq[String]]] = Map()
  private var _input: (Seq[Reader.Main.InputHappensAt], Seq[Reader.Main.InputHoldsAt], Seq[Reader.Main.InputHoldsFor]) = _
  private var _pendingInitiations: Iterable[((Data.FluentId, Seq[String]), Set[Long])] = Iterable()
  private var _nonTerminatedFluents: Map[Data.FluentId, Map[Seq[String], Data.Intervals]] = Map()

  def run(staticData: (Set[Data.InstantEvent], Set[Data.Fluent], (Seq[Data.IEPredicate], Seq[Data.FPredicate]), (Set[Data.InputEntity], Seq[Data.BuiltEntity]), Seq[(Data.EventId, String)]),
          input: (Seq[Reader.Main.InputHappensAt], Seq[Reader.Main.InputHoldsAt], Seq[Reader.Main.InputHoldsFor]),
          outputFile: String,
          start: Long, end: Long,
          clock: Long, eventTimestamps: Map[(String,Int), Map[Seq[String],Long]] = Map()): (Long,Long,String) = {

    // Set shared parameters
    val iEMapping: Map[Data.InstantEventId, Data.IEType] = staticData._1
      .map { iE =>
        iE.id -> iE.eventType
      }(collection.breakOut)

    val fluentMapping: Map[Data.FluentId, Data.FluentType] = staticData._2
      .map { fluent =>
        fluent.id -> fluent.eventType
      }(collection.breakOut)

    _windowDB = new EventDB(iEMapping, fluentMapping)
    _input = input
    _iEs = staticData._1
    _fluents = staticData._2
    _iEPredicates = staticData._3._1
    _fPredicates = staticData._3._2
    _inputEntities = staticData._4._1
    _builtEntities = staticData._4._2
    _cachingOrder = staticData._5
    _clock = clock
    _start = start
    _end = end

    // Update Input data
    processInput()

    val s = System.currentTimeMillis
    // Produce entities
    groundEntities()
    val t = System.currentTimeMillis

    // Produce and update built events
    processCE(eventTimestamps)
    val e = System.currentTimeMillis
    val recognitionTime = e - s


    /*val fd = new io.FileWriter(outputFile, true)
    fd.write(_windowDB.toJson)
    fd.close*/

    // return recognition time
    (recognitionTime,(t-s),_windowDB.output)
  }

  private def processInput(): Unit = {
    // Process HappensAt input
    val inputHappensAt: Seq[(Data.InstantEventId, Seq[String], Long)] = _input._1

    val formattedHappensAt: Iterable[((Data.InstantEventId, Seq[String]), Set[Long])] = inputHappensAt
      .groupBy { group =>
        (group._1, group._2)
      }
      .mapValues(_.map(_._3)(collection.breakOut))

    // Process HoldsAt input
    val inputHoldsAt: Seq[(Data.FluentId, Seq[String], Long)] = _input._2

    val formattedHoldsAt: Iterable[((Data.FluentId, Seq[String]), Data.Intervals)] = inputHoldsAt
      .groupBy { group =>
        (group._1, group._2)
      }
      .mapValues(v => Data.Intervals.fromPoints(v.map(_._3)(collection.breakOut), _clock))

    // Process HoldsFor input
    val inputHoldsFor: Seq[(Data.FluentId, Seq[String], Data.Intervals)] = _input._3

    val formattedHoldsFor: Iterable[((Data.FluentId, Seq[String]), Data.Intervals)] = inputHoldsFor
      .groupBy { group =>
        (group._1, group._2)
      }
      .mapValues(v => Data.Intervals.union(v.map(_._3)))

    // Update database
    _windowDB.updateIE(formattedHappensAt)
    _windowDB.updateFluent(formattedHoldsAt, false)
    _windowDB.updateFluent(formattedHoldsFor, false)
  }

  /**
    * Dynamic Grounding
    */
  private def groundEntities(): Unit = {
    // First produce input entities
    _entities = _inputEntities.map(_.instances(_windowDB))(collection.breakOut)

    // Then produce built entities
    _entities = _builtEntities.foldLeft(_entities) { (acc, entity) =>
      acc + entity.instances(acc)
    }

    // Now update entities from previous window
    _previousWindowEntities.foreach {
      case (name, entities) =>
        if (_entities.contains(name)) {
          // if current window contains entity, update it
          val newEntities = _entities(name) ++ entities
          _entities += (name -> newEntities.toVector.distinct)
        }
        else {
          // if current window does not contain current entity
          // just add the entity
          _entities += (name -> entities)
        }
    }
  }

  /**
    * Complex Event Recognition
    */
  private def processCE(eventTimestamps: Map[(String,Int), Map[Seq[String],Long]] = Map()): Unit = {
    _windowDB.amalgamate(_nonTerminatedFluents)
    _cachingOrder foreach {
      case (iEId: Data.InstantEventId, entityId) =>
        // Case: Instant Event
        _iEPredicates foreach {
          case p: Data.IEPredicate if p.id == iEId =>
            var name = ""
            var arguments: Seq[String] = Seq()
            var arity = 0
            if (p.body.head.isInstanceOf[HappensAtIE]) { // find out which is the first low level event to be validated
              val ha = p.body.head.asInstanceOf[HappensAtIE]
              name = ha.id.name
              arguments = ha.entity
              arity = arguments.size
            }
            val results: Iterable[((Data.InstantEventId, Seq[String]), Set[Long])] = p.validate(_windowDB, _entities(entityId))
            _windowDB.updateIE(results)
            val ingestionTimestamp = eventTimestamps((name,arity))(arguments)
            val ce = new ComplexEvent(_windowDB.toJson,ingestionTimestamp)
            sendToKafka(ce)
          case _ =>
        }

      case (fluentId: Data.FluentId, entityId) =>
        // Case: Fluent

        // get initiation points for current fluent
        var (initiations, left) = _pendingInitiations.partition(_._1._1.name == fluentId.name)
        _pendingInitiations = left
        var terminations: Iterable[((Data.FluentId, Seq[String]), Set[Long])] = Vector()
        // separate terminatedAt predicates
        val (terminatedPredicates, restPredicates) = _fPredicates.partition(_.isInstanceOf[Data.TerminatedAtPredicate])

        var name = ""
        var arguments: Seq[String] = Seq()
        var arity = 0

        restPredicates foreach {
          case p: Data.InitiatedAtPredicate =>
            if (p.id == fluentId) {
              if (p.body.head.isInstanceOf[HappensAtIE]) { // find out which is the first low level event to be validated
                val ha = p.body.head.asInstanceOf[HappensAtIE]
                name = ha.id.name
                arguments = ha.entity
                arity = arguments.size
              }
              initiations ++= p.validate(_windowDB, _entities(entityId))
            }
          case p: Data.SDFPredicate =>
            if (p.id == fluentId) {
              if (p.body.head.isInstanceOf[HappensAtIE]) { // find out which is the first low level event to be validated
                val ha = p.body.head.asInstanceOf[HappensAtIE]
                name = ha.id.name
                arguments = ha.entity
                arity = arguments.size
              }
              val result = p.validate(_windowDB, _entities(entityId))
              _windowDB.updateFluent(result, true)
            }
        }

        if (initiations.nonEmpty) {
          // only if there are initiations, start evaluating terminatedAt predicates
          terminatedPredicates foreach {
            case p: Data.TerminatedAtPredicate =>
              if (p.id == fluentId)
                terminations ++= p.validate(_windowDB, _entities(entityId))
          }
          _simpleFluentsNames ++= initiations.map(_._1._1.name)
          val combined: Iterable[((Data.FluentId, Seq[String]), Data.Intervals)] = combineSF(initiations, terminations)
          _windowDB.updateFluent(combined, false)
        }
        val ingestionTimestamp = eventTimestamps((name,arity))(arguments)
        val ce = new ComplexEvent(_windowDB.toJson,ingestionTimestamp)
        sendToKafka(ce)
    }
    retractFluents
    retractEntities
  }

  def sendToKafka(output: ComplexEvent): Unit = {
    val  props = new Properties()
    props.put("bootstrap.servers", "192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", classOf[ComplexEvent_KryoSerializer].getName)
    props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,classOf[ComplexEventProducerInterceptor].getName)

    val producer = new KafkaProducer[String, ComplexEvent](props)

    val TOPIC="sample_cer_topic"

    val record = new ProducerRecord(TOPIC, "localhost", output)
    producer.send(record)

    producer.close
  }

  private def combineSF(initiations: Iterable[((Data.FluentId, Seq[String]), Set[Long])], terminations: Iterable[((Data.FluentId, Seq[String]), Set[Long])]): Iterable[((Data.FluentId, Seq[String]), Data.Intervals)] = {
    val init: Map[(Data.FluentId, Seq[String]), Set[Long]] = initiations
      .groupBy(_._1)
      .mapValues(_.flatMap(_._2)(collection.breakOut))

    val term: Map[(Data.FluentId, Seq[String]), Set[Long]] = terminations
      .groupBy(_._1)
      .mapValues(_.flatMap(_._2)(collection.breakOut))

    val ids: Set[Data.FluentId] = init.keySet map (_._1)

    init map { i: ((Data.FluentId, Seq[String]), Set[Long]) =>
      val otherIds: Set[Data.FluentId] = ids - i._1._1
      val entityInitiations: Set[Long] = i._2

      val entityTerminations: Set[Long] = term.getOrElse(i._1, Set()) ++ {
        otherIds flatMap { id =>
          init.getOrElse((id, i._1._2), Set())
        }
      }

      // calculate fluents with their intervals
      val fluents = i._1 -> Data.Intervals.combine(entityInitiations, entityTerminations, _clock)
      if (fluents._2.t.nonEmpty) {
        // get fluent's last interval
        val lastIntervals = fluents._2.t.last
        // if it is non terminated, add it to pending initiations
        if (lastIntervals._2 == -1) _pendingInitiations = _pendingInitiations ++ Iterable((fluents._1, Set(lastIntervals._1 - _clock)))
      }
      // return fluents
      fluents
    }
  }

  /**
    * Gets the number of complex events
    * @return
    */
  def numComplexEvents: Int = {
    _windowDB.getNumCEs
  }

  /**
    * Gets non terminated simple fluents
    * @return initiation points
    */
  def getNonTerminatedSimpleFluents: Iterable[((Data.FluentId, Seq[String]), Set[Long])] = {
    _pendingInitiations
  }

  /**
    * Updates reasoner's initiation point list
    * @param previousInitiations
    */
  def updateNonTerminatedInitiations(previousInitiations: Iterable[((Data.FluentId, Seq[String]), Set[Long])]): Unit = {
    _pendingInitiations = previousInitiations
  }

  /**
    * Retracts non terminated sdfluents from current window
    */
  def retractFluents: Unit = {
    _nonTerminatedFluents = _windowDB.getNonTerminatedFluents
  }

  /**
    * Gets all the non terminated sdfluents
    * @return fluents
    */
  def getNonTerminatedSDFluents: Map[Data.FluentId, Map[Seq[String], Data.Intervals]] = {
    _nonTerminatedFluents.filterNot(f => _simpleFluentsNames.contains(f._1.name))
  }

  /**
    * Updates non terminated
    * sd fluents from previous windows
    * @param sdFluents
    */
  def updateSDFluents(sdFluents: Map[Data.FluentId, Map[Seq[String], Data.Intervals]]): Unit = {
    _nonTerminatedFluents = sdFluents
  }

  /**
    * Gets previous window entities and
    * previous window entities
    * of non terminated fluents
    * @return (previous window entities, prev win entities non terminated)
    */
  def retractEntities: Unit = {
    val entities = _nonTerminatedFluents.flatMap(_._2.map(_._1))
    if (entities.nonEmpty) {
      val numArgs = entities.head.size
      _previousWindowEntities = _entities.filter {
        e =>
          if (e._2.nonEmpty)
            e._2.head.size == numArgs
          else
            e._1 != ""
      }
    }
    else {
      _previousWindowEntities = Map()
    }
  }

  /**
    * Gets previous window entities
    * @return
    */
  def getPreviousWindowEntities: Map[String, Iterable[Seq[String]]] = _previousWindowEntities

  /**
    * Updates previous window entities
    * @param e previous window entities
    */
  def updateCurrentWindowEntities(e: Map[String, Iterable[Seq[String]]]): Unit = {
    _previousWindowEntities = e
  }

}
