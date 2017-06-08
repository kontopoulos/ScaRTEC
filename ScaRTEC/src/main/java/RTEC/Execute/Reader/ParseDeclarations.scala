package RTEC.Execute.Reader

import RTEC._

import scala.util.parsing.combinator.JavaTokenParsers

object ParseDeclarations extends JavaTokenParsers {

    private def alphanumeric = ident | decimalNumber


    private def instantEvent: Parser[Data.InstantEvent] = (("Input" | "Internal" | "Output") <~ ":") ~ ("[" ~> alphanumeric) ~ (decimalNumber <~ "]") ^^ {
        case ("Input" ~ name ~ noa) =>
            Data.InstantEvent(Data.InputIE, name, noa.toInt)
        case ("Internal" ~ name ~ noa) =>
            Data.InstantEvent(Data.InternalIE, name, noa.toInt)
        case ("Output" ~ name ~ noa) =>
            Data.InstantEvent(Data.OutputIE, name, noa.toInt)
    }
    private def instantEvents: Parser[Set[Data.InstantEvent]] = ("InstantEvents" ~ "{") ~> rep(instantEvent) <~ "}" ^^ {
        case events => events.toSet
    }

    private def fluent: Parser[Data.Fluent] = (("Simple" | "InputSD" | "InternalSD" | "OutputSD") <~ ":") ~ ("[" ~> alphanumeric) ~ decimalNumber ~ ("=" ~> alphanumeric <~ "]") ^^ {
        case ("Simple" ~ name ~ noa ~ value) =>
            Data.Fluent(Data.SimpleFluent, name, noa.toInt, value)
        case ("InputSD" ~ name ~ noa ~ value) =>
            Data.Fluent(Data.InputSDFluent, name, noa.toInt, value)
        case ("InternalSD" ~ name ~ noa ~ value) =>
            Data.Fluent(Data.InternalSDFluent, name, noa.toInt, value)
        case ("OutputSD" ~ name ~ noa ~ value) =>
            Data.Fluent(Data.OutputSDFluent, name, noa.toInt, value)
    }
    private def fluents: Parser[Set[Data.Fluent]] = ("Fluents" ~ "{") ~> rep(fluent) <~ "}" ^^ {
        case events => events.toSet
    }

    private def inputEntity: Parser[Data.InputEntity] = ((ident  ~ decimalNumber) <~ ":") ~ rep1("[" ~> ident ~ opt("=" ~> ident) <~ "]") ^^ {
        case (name ~ numOfArgs ~ sources) =>
            val noa = numOfArgs.toInt
            val s = sources map {
                case (n ~ None) =>
                    Data.InstantEventId(n, noa)
                case (n ~ Some(value)) =>
                    Data.FluentId(n, noa, value)
            }

            Data.InputEntity(name, noa, s)
    }
    private def inputEntities: Parser[Set[Data.InputEntity]] = ("InputEntities" ~ "{") ~> rep(inputEntity) <~ "}" ^^ {
        case entities => entities.toSet
    }

    private def builtEntities: Parser[Seq[Data.BuiltEntity]] = ("BuiltEntities" ~ "{") ~> rep(builtEntity) <~ "}"
    
    private def builtEntity: Parser[Data.BuiltEntity] = ((ident ~ decimalNumber) <~ ":") ~ rep1("[" ~> entitySource <~ "]") ^^ {
        case (name ~ numOfArgs ~ sources) =>
            Data.BuiltEntity(name, numOfArgs.toInt, sources)
    }
    private def entitySourceArg: Parser[(Option[String], Seq[String])] = opt(alphanumeric) ~ ("(" ~> repsep(alphanumeric, ",") <~ ")") ^^ {
        case (name ~ args) =>
            (name, args)
    }
    private def entitySource: Parser[Seq[(Option[String], Seq[String])]] = rep1(entitySourceArg)
    private def entities: Parser[(Set[Data.InputEntity], Seq[Data.BuiltEntity])] = {
        inputEntities ~ builtEntities ^^ {
            case (ie ~ be) =>
                (ie, be)
        }
    }

    private def eventId: Parser[Data.EventId] = "[" ~> alphanumeric ~ decimalNumber ~ opt("=" ~> alphanumeric) <~ "]" ^^ {
        case (name ~ noa ~ None) =>
            Data.InstantEventId(name, noa.toInt)
        case (name ~ noa ~ Some(value)) =>
            Data.FluentId(name, noa.toInt, value)
    }
    private def cachingItem: Parser[(Data.EventId, String)] = eventId ~ ("->" ~> ident) ^^ {
        case (ev ~ entityId) => (ev, entityId)
    }
    private def cachingOrder: Parser[Seq[(Data.EventId, String)]] = ("CachingOrder" ~ "{") ~> rep1(cachingItem) <~ "}"

    private def all: Parser[(Set[Data.InstantEvent], Set[Data.Fluent], (Set[Data.InputEntity], Seq[Data.BuiltEntity]), Seq[(Data.EventId, String)])] =
        instantEvents ~ fluents ~ entities ~ cachingOrder ^^ {
            case (iEs ~ fl ~ ent ~ co) =>
                (iEs, fl, ent, co)
        }

        def get(source: String): Option[(Set[Data.InstantEvent], Set[Data.Fluent], (Set[Data.InputEntity], Seq[Data.BuiltEntity]), Seq[(Data.EventId, String)])] = {
        parseAll(all, source) match {
            case Success((iEs, fluents, entities, caching), _) => Some((iEs, fluents, entities, caching))
            case NoSuccess(errorMessage, _) => println(s"(Declarations) Parsing failed: $errorMessage"); None
        }
    }

}
