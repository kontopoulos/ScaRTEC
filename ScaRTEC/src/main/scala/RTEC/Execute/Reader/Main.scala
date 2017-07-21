package RTEC.Execute.Reader

import RTEC._

object Main {

    type InputHappensAt = (Data.InstantEventId, Seq[String], Long)
    type InputHoldsAt = (Data.FluentId, Seq[String], Long)
    type InputHoldsFor = (Data.FluentId, Seq[String], Data.Intervals)

    private var _iEs: Option[Set[Data.InstantEvent]] = None
    private var _fluents: Option[Set[Data.Fluent]] = None
    private var _predicates: Option[(Seq[Data.IEPredicate], Seq[Data.FPredicate])] = None
    private var _entities: Option[(Set[Data.InputEntity], Seq[Data.BuiltEntity])] = None
    private var _cachingOrder: Option[Seq[(Data.EventId, String)]] = None

    def staticData: Option[(Set[Data.InstantEvent],
                     Set[Data.Fluent],
                     (Seq[Data.IEPredicate], Seq[Data.FPredicate]),
                     (Set[Data.InputEntity], Seq[Data.BuiltEntity]),
                     Seq[(Data.EventId, String)])] = {

        (_iEs, _fluents, _predicates, _entities, _cachingOrder) match {
            case (Some(iEs), Some(fluents), Some(predicates), Some(entities), Some(cachingOrder)) =>
                Some((iEs, fluents, predicates, entities, cachingOrder))
            case _ => None
        }

    }

    def readDeclarations(file: String): Unit = {
        val source = scala.io.Source.fromFile(file).mkString
        ParseDeclarations.get(source) match {
            case Some((iEs, fluents, entities, cachingOrd)) =>
                _iEs = Some(iEs)
                _fluents = Some(fluents)
                _cachingOrder = Some(cachingOrd)
                _entities = Some(entities)
        }
    }

    def readDefinitions(file: String): Unit = {
        val source = scala.io.Source.fromFile(file).mkString
        _predicates = ParseDefinitions.get(source, _fluents.get)

    }
}