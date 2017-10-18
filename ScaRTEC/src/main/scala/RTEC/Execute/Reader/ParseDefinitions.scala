package RTEC.Execute.Reader

import RTEC._

import scala.util.parsing.combinator.JavaTokenParsers


object ParseDefinitions extends JavaTokenParsers {

  private var _fluents: Iterable[Data.Fluent] = null
  private var _iEPredicates = Vector[Data.IEPredicate]()
  private var _fPredicates = Vector[Data.FPredicate]()

  private def alphanumeric = ident | decimalNumber

  private def instantEventEntity: Parser[(Data.InstantEventId, Seq[String])] = "[" ~> ident ~ rep(alphanumeric) <~ "]" ^^ {
    case (name ~ args) =>
      (Data.InstantEventId(name, args.length), args)
  }

  private def fluentEntity: Parser[(Data.FluentId, Seq[String])] = ("[" ~> ident) ~ rep(alphanumeric) ~ ("=" ~> alphanumeric) <~ "]" ^^ {
    case (name ~ args ~ value) =>
      (Data.FluentId(name, args.length, value), args)
  }

  private def interval: Parser[(Int, Int)] = decimalNumber ~ ("-" ~> decimalNumber) ^^ {
    case (n1 ~ n2) => (n1.toInt, n2.toInt)
  }

  private def intervals: Parser[String] = "(" ~> rep1(interval) <~ ")" ^^ {
    case lIntervals => Data.Intervals(lIntervals.toVector).toString
  }

  private def happensAtFluentStart: Parser[Data.HappensAtFluentStart] = ("HappensAt" ~ "Start") ~> fluentEntity ~ ident ^^ {
    case (entity ~ time) => Data.HappensAtFluentStart(entity._1, entity._2, time)
  }

  private def happensAtFluentEnd: Parser[Data.HappensAtFluentEnd] = ("HappensAt" ~ "End") ~> fluentEntity ~ ident ^^ {
    case (entity ~ time) => Data.HappensAtFluentEnd(entity._1, entity._2, time)
  }

  private def holdsAt: Parser[Data.HoldsAt] = "HoldsAt" ~> fluentEntity ~ ident ^^ {
    case (entity ~ time) => Data.HoldsAt(entity._1, entity._2, time)
  }

  private def notHoldsAt: Parser[Data.NotHoldsAt] = ("Not" ~ "HoldsAt") ~> fluentEntity ~ ident ^^ {
    case (entity ~ time) => Data.NotHoldsAt(entity._1, entity._2, time)
  }

  private def unionAll: Parser[Data.UnionAll] = "Union_All" ~> opt("!") ~ ("[" ~> rep1(intervals | ident) <~ "]") ~ ident ^^ {
    case (Some("!") ~ arg1 ~ arg2) => Data.UnionAll(arg1, arg2, strict = true)
    case (None ~ arg1 ~ arg2) => Data.UnionAll(arg1, arg2, strict = false)
  }

  private def intersectAll: Parser[Data.IntersectAll] = "Intersect_All" ~> opt("!") ~ ("[" ~> rep1(intervals | alphanumeric) <~ "]") ~ alphanumeric ^^ {
    case (Some("!") ~ arg1 ~ arg2) => Data.IntersectAll(arg1, arg2, strict = true)
    case (None ~ arg1 ~ arg2) => Data.IntersectAll(arg1, arg2, strict = false)
  }

  private def complementAll: Parser[Data.ComplementAll] = "Complement_All" ~> ("[" ~> rep1(intervals | alphanumeric) <~ "]") ~ alphanumeric ^^ {
    case (arg1 ~ arg2) => Data.ComplementAll(arg1, arg2)
  }

  private def relativeComplementAll: Parser[Data.RelativeComplementAll] =
    "Relative_Complement_All" ~> opt("!") ~ (intervals | ident) ~ ("[" ~> rep1(intervals | ident) <~ "]") ~ ident ^^ {
      case (Some("!") ~ arg1 ~ arg2 ~ arg3) => Data.RelativeComplementAll(arg1, arg2, arg3, strict = true)
      case (None ~ arg1 ~ arg2 ~ arg3) => Data.RelativeComplementAll(arg1, arg2, arg3, strict = false)
    }

  /* ============ Maritime Domain ============ */

  private def notNearPorts: Parser[Data.NotNearPorts] = "NotNearPorts" ~> alphanumeric ~ alphanumeric ^^ {
    case (lon ~ lat) => Data.NotNearPorts(lon, lat)
  }

  private def nearPorts: Parser[Data.NearPorts] = "NearPorts" ~> alphanumeric ~ alphanumeric ^^ {
    case (lon ~ lat) => Data.NearPorts(lon, lat)
  }

  private def inArea: Parser[Data.InArea] = "InArea" ~> alphanumeric ~ alphanumeric ^^ {
    case (lon ~ lat) => Data.InArea(lon, lat)
  }

  private def notInPorts: Parser[Data.NotInPorts] = "NotInPorts" ~> alphanumeric ~ alphanumeric ~ alphanumeric ~ alphanumeric ~ alphanumeric ~ alphanumeric ^^ {
    case (entityId ~ lon ~ lat ~ timePoint ~ iInterval ~ finalInterval) => Data.NotInPorts(entityId, lon, lat, timePoint, iInterval, finalInterval)
  }

  private def thresholdGreater: Parser[Data.ThresholdGreater] = "ThresholdGreater" ~> alphanumeric ~ alphanumeric ^^ {
    case (speed ~ lowerLimit) => Data.ThresholdGreater(speed, lowerLimit)
  }

  private def thresholdLess: Parser[Data.ThresholdLess] = "ThresholdLess" ~> alphanumeric ~ alphanumeric ^^ {
    case (speed ~ lowerLimit) => Data.ThresholdLess(speed, lowerLimit)
  }

  private def intDurGreater: Parser[Data.IntDurGreater] = "IntDurGreater" ~> alphanumeric ~ alphanumeric ^^ {
    case (interval ~ duration) => Data.IntDurGreater(interval, duration)
  }

  private def inAreaSpeedGreater: Parser[Data.InAreaSpeedGreater] = "SpeedInAreaGreater" ~> alphanumeric ~ alphanumeric ^^ {
    case (area ~ speed) => Data.InAreaSpeedGreater(area, speed)
  }

  private def inAreaSpeedLess: Parser[Data.InAreaSpeedLess] = "SpeedInAreaLess" ~> alphanumeric ~ alphanumeric ^^ {
    case (area ~ speed) => Data.InAreaSpeedLess(area, speed)
  }

  /* ============ Maritime Domain ============ */

  // HappensAt | HoldsFor | Union_All | Intersect_All | Relative_Complement_All
  private def bodyClause: Parser[Data.BodyClause] =
    happensAtIE | happensAtFluentStart | happensAtFluentEnd | holdsFor | holdsAt | notHappensAtIE | notHoldsAt |
      unionAll | intersectAll | relativeComplementAll | complementAll | notNearPorts | nearPorts | inArea | notInPorts |
      thresholdGreater | thresholdLess | intDurGreater | inAreaSpeedGreater | inAreaSpeedLess

  // HappensAt <instantEvent> <args> <time>
  private def happensAtIE: Parser[Data.HappensAtIE] = "HappensAt" ~> instantEventEntity ~ alphanumeric ^^ {
    case (entity ~ time) => Data.HappensAtIE(entity._1, entity._2, time)
  }

  private def notHappensAtIE: Parser[Data.NotHappensAtIE] = ("Not" ~ "HappensAt") ~> instantEventEntity ~ alphanumeric ^^ {
    case (entity ~ time) => Data.NotHappensAtIE(entity._1, entity._2, time)
  }

  private def happensAtPredicate: Parser[Unit] = (">" ~> happensAtIE) ~ rep(bodyClause) ^^ {
    case (head ~ body) =>
      _iEPredicates :+= Data.HappensAtPredicate(head, body.toVector)
  }

  // InitiatedAt <simpleFluent> <time>
  private def initiatedAt: Parser[Data.InitiatedAt] = "InitiatedAt" ~> fluentEntity ~ ident ^^ {
    case (entity ~ time) =>
      Data.InitiatedAt(entity._1, entity._2, time)
  }

  private def initiatedAtPredicate: Parser[Unit] = (">" ~> initiatedAt) ~ rep(bodyClause) ^^ {
    case (head ~ body) =>
      val addition = Data.InitiatedAtPredicate(head, body)
      if (Data.Clause.isVariable(head.id.value)) {
        val id = Data.FluentId(head.id.name, head.id.numOfArgs, "_")
        val values = _fluents
          .filter(f => (f.eventType == Data.SimpleFluent) && f.id == id)
          .map(_.value)

        _fPredicates ++= values map addition.groundOnValue


      } else
        _fPredicates :+= addition
  }

  // TerminatedAt <simpleFluent> <time>
  private def terminatedAt: Parser[Data.TerminatedAt] = "TerminatedAt" ~> fluentEntity ~ ident ^^ {
    case (entity ~ time) =>
      Data.TerminatedAt(entity._1, entity._2, time)
  }

  private def terminatedAtPredicate: Parser[Unit] = (">" ~> terminatedAt) ~ rep(bodyClause) ^^ {
    case (head ~ body) =>
      val addition = Data.TerminatedAtPredicate(head, body)
      if (Data.Clause.isVariable(head.id.value)) {
        val id = Data.FluentId(head.id.name, head.id.numOfArgs, "_")
        val values = _fluents
          .filter(f => (f.eventType == Data.SimpleFluent) && f.id == id)
          .map(_.value)

        _fPredicates ++= values map addition.groundOnValue


      } else
        _fPredicates :+= addition
  }

  // HoldsFor <simpleFluent> <timeIdentifier | timeIntervals>
  private def holdsFor: Parser[Data.HoldsFor] = "HoldsFor" ~> opt("!") ~ fluentEntity ~ ident ^^ {
    case (Some("!") ~ entity ~ time) =>
      Data.HoldsFor(entity._1, entity._2, time, strict = true)
    case (None ~ entity ~ time) =>
      Data.HoldsFor(entity._1, entity._2, time, strict = false)
  }

  private def holdsForPredicate: Parser[Unit] = (">" ~> holdsFor) ~ rep(bodyClause) ^^ {
    case (head ~ body) =>
      val addition = Data.HoldsForPredicate(head, body)
      if (Data.Clause.isVariable(head.id.value)) {
        val id = Data.FluentId(head.id.name, head.id.numOfArgs, "_")
        val values = _fluents
          .filter(f => (f.eventType == Data.InternalSDFluent || f.eventType == Data.OutputSDFluent) && f.id == id)
          .map(_.value)

        _fPredicates ++= values map addition.groundOnValue


      } else
        _fPredicates :+= addition
  }

  private def all: Parser[(Seq[Data.IEPredicate], Seq[Data.FPredicate])] = rep(happensAtPredicate | initiatedAtPredicate | terminatedAtPredicate | holdsForPredicate) ^^ {
    case _ =>
      (_iEPredicates, _fPredicates)
  }

  def get(source: String, fluents: Iterable[Data.Fluent]): Option[(Seq[Data.IEPredicate], Seq[Data.FPredicate])] = {
    _fluents = fluents
    parseAll(all, source) match {
      case Success(results, _) =>
        Some(results)
      case NoSuccess(errorMessage, _) =>
        println(s"(Definitions): Parsing failed: $errorMessage")
        None
      case _ =>
        None
    }
  }
}
