package RTEC.Data

import RTEC._

object Predicate {
    type GroundingDict = (Seq[String], Map[String, String], Map[String, Intervals], Map[String, Set[Int]])
}

sealed abstract class Predicate(val head: HeadClause, val body: Seq[BodyClause]) {
    def id: EventId

    def assign(entities: Iterable[Seq[String]]): Iterable[Predicate.GroundingDict] = {
        entities
            .map {entity =>
                val pattern = head.entity
                val zipped = pattern zip entity
                val conflict = zipped exists {label =>
                    !Clause.isVariable(label._1) && !Clause.isWildCard(label._1) && label._1 != label._2
                }
                if (conflict)
                    null

                else {
                    val entityDict = pattern
                        .zip(entity)
                        .filter {label =>
                            Clause.isVariable(label._1)
                        }
                        .toMap

                    (entity, entityDict, Map[String, Intervals](), Map[String, Set[Int]]())
                }
            }
            .filter(_ != null)
    }
}

sealed abstract class IEPredicate(override val head: HeadClause, override val body: Seq[BodyClause])
    extends Predicate(head, body) {

    override def id: InstantEventId
    def validate(data: Execute.EventDB, entities: Iterable[Seq[String]]): Iterable[((Data.InstantEventId, Seq[String]), Set[Int])]
}


sealed abstract class FPredicate(override val head: HeadClause, override val body: Seq[BodyClause])
    extends Predicate(head, body) {

    override def id: FluentId
    def groundOnValue(value: String): FPredicate
}

sealed abstract class SFPredicate(override val head: HeadClause, override val body: Seq[BodyClause])
    extends FPredicate(head, body) {

    def validate(data: Execute.EventDB, entities: Iterable[Seq[String]]): Iterable[((Data.FluentId, Seq[String]), Set[Int])]
    override def groundOnValue(value: String): SFPredicate
}

sealed abstract class SDFPredicate(override val head: HeadClause, override val body: Seq[BodyClause])
    extends FPredicate(head, body) {

    def validate(data: Execute.EventDB, entities: Iterable[Seq[String]]): Iterable[((Data.FluentId, Seq[String]), Intervals)]
    override def groundOnValue(value: String): SDFPredicate
}

case class HappensAtPredicate(override val head: HappensAtIE, override val body: Seq[BodyClause])
    extends IEPredicate(head, body) {

    override val id: InstantEventId = head.id

    override def validate(data: Execute.EventDB, entities: Iterable[Seq[String]]): Iterable[((Data.InstantEventId, Seq[String]), Set[Int])] = {
        var dicts = assign(entities)

        body foreach { clause =>
            dicts = clause.resolve(data, dicts)
            if (dicts.isEmpty) {
                return Set()
            }
        }

        val timeLabel = head.time
        dicts collect {case (entity, _, _, time) if time(timeLabel).nonEmpty =>
            ((id, entity), time(timeLabel))
        }
    }
}

case class InitiatedAtPredicate(override val head: InitiatedAt, override val body: Seq[BodyClause])
    extends SFPredicate(head, body) {

    override def id: FluentId = head.id

    override def validate(data: Execute.EventDB, entities: Iterable[Seq[String]]): Iterable[((Data.FluentId, Seq[String]), Set[Int])] = {
        var dicts = assign(entities)

        body foreach {clause =>
            dicts = clause.resolve(data, dicts)
            if (dicts.isEmpty) {
                return Set()
            }
        }

        val timeLabel = head.time
        dicts collect {case (entity, _, _, time) if time(timeLabel).nonEmpty =>
            ((id, entity), time(timeLabel))
        }
    }

    override def groundOnValue(value: String): InitiatedAtPredicate = {
        val target = head.id.value
        val h = head.replaceLabel(target, value)
        val b = body map (_.replaceLabel(target, value))

        InitiatedAtPredicate(h, b)
    }
}

case class TerminatedAtPredicate(override val head: TerminatedAt, override val body: Seq[BodyClause])
    extends SFPredicate(head, body) {

    override def id: FluentId = head.id


    override def validate(data: Execute.EventDB, entities: Iterable[Seq[String]]): Iterable[((Data.FluentId, Seq[String]), Set[Int])] = {
        var dicts = assign(entities)

        body foreach {clause =>
            dicts = clause.resolve(data, dicts)
            if (dicts.isEmpty) {
                return Set()
            }
        }

        val timeLabel = head.time
        dicts collect {case (entity, _, _, time) if time(timeLabel).nonEmpty =>
            ((id, entity), time(timeLabel))
        }
    }

    override def groundOnValue(value: String): TerminatedAtPredicate = {
        val target = head.id.value
        val h = head.replaceLabel(target, value)
        val b = body map (_.replaceLabel(target, value))

        TerminatedAtPredicate(h, b)
    }
}

case class HoldsForPredicate(override val head: HoldsFor, override val body: Seq[BodyClause])
    extends SDFPredicate(head, body) {

    override def id: FluentId = head.id

    override def validate(data: Execute.EventDB, entities: Iterable[Seq[String]]): Iterable[((Data.FluentId, Seq[String]), Intervals)] = {
        var dicts = assign(entities)

        body foreach {clause =>
            dicts = clause.resolve(data, dicts)
            if (dicts.isEmpty) {
                return Set()
            }
        }

        val timeLabel = head.time
        dicts collect {case (entity, _, time, _) if time(timeLabel).nonEmpty =>
            ((id, entity), time(timeLabel))
        }
    }

    override def groundOnValue(value: String): HoldsForPredicate = {
        val target = head.id.value
        val h = head.replaceLabel(target, value)
        val b = body map (_.replaceLabel(target, value))

        HoldsForPredicate(h, b)
    }
}

