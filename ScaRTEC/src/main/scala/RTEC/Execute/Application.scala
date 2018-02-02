package RTEC.Execute

import org.apache.log4j.varia.NullAppender

object Application extends App {

  override def main(args: Array[String]): Unit = {

    // all areas
    /*val e1 = "@prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e . @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e . @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e . @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e . @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e . @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e .   @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .  @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .  @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .  @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e .  @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e .  @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e . @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e . @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e . @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e . @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e . @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e . @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e .   @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .  @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .  @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .  @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e .  @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e .  @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e . :FAOzone_27_7_e a :FAO_area ;  :hasPlaceName \\\"Western English Channel\\\" ;  rdfs:seeAlso \\\"http://www.fao.org/fishery/area/Area27/en#FAO-fishing-area-27.7.e\\\" ; :hasGeometry \\u003chttp://83.212.239.107/geometries/ShapeFiles_FAO_Europe/53\\u003e .  \\u003chttp://83.212.239.107/geometries/ShapeFiles_FAO_Europe/53\\u003e :hasMBR_WKT \\\"POLYGON ((-6.9999999539 48.0000000151, -6.9999999539 51.77513, -1.36889 51.77513, -1.36889 48.0000000151, -6.9999999539 48.0000000151))\\\" ."

    val e2 = "@prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e . @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e . @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e . @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e . @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e . @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e .   @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .  @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .  @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .  @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e .  @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e .  @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e . @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e . @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e . @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e . @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e . @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e . @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e .   @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .  @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .  @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .  @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e .  @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e .  @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e . @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e . @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e . @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e . @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e . @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e . @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e .   @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .  @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .  @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .  @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e .  @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e .  @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e . :natura_NL1000012 a :Natura2000_zone ;  :hasCountryCode \\\"NL\\\" ;  :partOfCountry :Place_Netherlands ;  :hasPlaceName \\\"Kennemerland-Zuid\\\" ;  :hasGeometry \\u003chttp://83.212.239.107/geometries/Natura2000/9\\u003e .  \\u003chttp://83.212.239.107/geometries/Natura2000/9\\u003e :hasMBR_WKT \\\"POLYGON ((4.472449450275294 52.298419295517775, 4.472449450275294 52.4566641225577, 4.566467512283134 52.4566641225577, 4.566467512283134 52.298419295517775, 4.472449450275294 52.298419295517775))\\\" ."

    val area1 = e1.split("[:]").filter(_.contains("FAOzone")).head.dropRight(3)
    println("areaId_" + area1)

    val area2 = e2.split("[:]").filter(_.contains("natura")).head.dropRight(3)
    println("areaId_" + area2)

    val w = new java.io.FileWriter("areas_speed_limits.csv")
    scala.io.Source.fromFile("final_output_correct.json").getLines.foreach{
      line =>
        if (line.contains("FAOzone") || line.contains("natura")) {
          val area = line.split("[:]").filter(x => x.contains("FAOzone") || x.contains("natura")).head.dropRight(3)
          val upperBound = (22.0 - 8.0) + 1.0
          val r = scala.util.Random
          val limit = 8.0 + r.nextInt(upperBound.toInt)
          w.write(s"areaId_$area|$limit\n")
        }
    }
    w.close*/


    // contains #within
    /*val e = "@prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e . @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e . @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e . @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e . @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e . @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e .   @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .  @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .  @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .  @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e .  @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e .  @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e . @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e . @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e . @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e . @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e . @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e . @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e .   @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .  @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .  @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .  @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e .  @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e .  @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e . \\u003chttp://www.datacron-project.eu/datAcron#node_228017700_1443942010000_-4.6657734_48.3296\\u003e \\u003chttp://www.datacron-project.eu/datAcron#within\\u003e \\u003chttp://www.datacron-project.eu/datAcron#FAOzone_27_7_e\\u003e ."

    val parts = e.split(" ").filterNot(_ == ".").takeRight(3)
    val vesselInfo = parts.head.split("[#]").last.split("[_]")
    val areaId = "areaId_" + parts.last.split("[#]").last.split("[\\\\]").head

    val id = vesselInfo(1)
    val ts = vesselInfo(2).toLong/1000
    val lon = vesselInfo(3)
    val lat = vesselInfo(4).split("[\\\\]").head

    println(id)
    println(ts)
    println(lon)
    println(lat)
    println(areaId)

    println(s"HappensAt [within $id $areaId] $ts")
    println(s"HappensAt [coord $id $lon $lat] $ts")*/

    // contains #nearTo
    /*val e = "@prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e . @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e . @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e . @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e . @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e . @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e .   @prefix rdf: \\u003chttp://www.w3.org/1999/02/22-rdf-syntax-ns#\\u003e .  @prefix rdfs: \\u003chttp://www.w3.org/2000/01/rdf-schema#\\u003e .  @prefix xsd:    \\u003chttp://www.w3.org/2001/XMLSchema#\\u003e .  @prefix unit:   \\u003chttp://datAcron-project.eu/units/\\u003e .  @prefix : \\u003chttp://www.datacron-project.eu/datAcron#\\u003e .  @prefix dul: \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#\\u003e . \\u003chttp://www.datacron-project.eu/datAcron#node_227005550_1443942055000_-4.521897_48.3381\\u003e \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#precedes\\u003e \\u003chttp://www.datacron-project.eu/datAcron#node_227005550_1443942124000_-4.52193_48.338367\\u003e . \\u003chttp://www.datacron-project.eu/datAcron#node_227005550_1443942124000_-4.52193_48.338367\\u003e \\u003chttp://www.ontologydesignpatterns.org/ont/dul/DUL.owl#nearTo\\u003e \\u003chttp://www.datacron-project.eu/datAcron#node_304655000_1443942124000_-4.5219765_48.338367\\u003e . "
    val parts = e.split(" ").filterNot(_ == ".").takeRight(3)

    val vessel1 = parts.head.split("[#]").last.split("[_]")
    val id1 = vessel1(1)
    val ts1 = vessel1(2).toLong/1000
    val lon1 = vessel1(3)
    val lat1= vessel1(4).split("[\\\\]").head

    val vessel2 = parts.last.split("[#]").last.split("[_]")
    val id2 = vessel2(1)
    val ts2 = vessel2(2).toLong/1000
    val lon2 = vessel2(3)
    val lat2 = vessel2(4).split("[\\\\]").head


    println(s"HappensAt [near $id1 $id2] $ts1")
    println(s"HappensAt [coord $id1 $lon1 $lat1] $ts1")
    println(s"HappensAt [coord $id2 $lon2 $lat2] $ts2")*/

    // contains critical point
    /*val annotation = Map("StoppedInit" -> "stop_start",
      "StoppedEnd" -> "stop_end",
      "SlowMotionStart" -> "slow_motion_start",
      "SlowMotionEnd" -> "slow_motion_end",
      "GapEnd" -> "gap_end",
      "HeadingChange" -> "change_in_heading",
      "SpeedChangeStart" -> "change_in_speed_start",
      "SpeedChangeEnd" -> "change_in_speed_end")

    val e = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . @prefix xsd:    <http://www.w3.org/2001/XMLSchema#> . @prefix unit:   <http://datAcron-project.eu/units/> . @prefix : <http://www.datacron-project.eu/datAcron#> . @prefix dul: <http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#> .   @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .  @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .  @prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .  @prefix unit:   <http://datAcron-project.eu/units/> .  @prefix : <http://www.datacron-project.eu/datAcron#> .  @prefix dul: <http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . @prefix xsd:    <http://www.w3.org/2001/XMLSchema#> . @prefix unit:   <http://datAcron-project.eu/units/> . @prefix : <http://www.datacron-project.eu/datAcron#> . @prefix dul: <http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#> .   @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .  @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .  @prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .  @prefix unit:   <http://datAcron-project.eu/units/> .  @prefix : <http://www.datacron-project.eu/datAcron#> .  @prefix dul: <http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . @prefix xsd:    <http://www.w3.org/2001/XMLSchema#> . @prefix unit:   <http://datAcron-project.eu/units/> . @prefix : <http://www.datacron-project.eu/datAcron#> . @prefix dul: <http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#> .   @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .  @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .  @prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .  @prefix unit:   <http://datAcron-project.eu/units/> .  @prefix : <http://www.datacron-project.eu/datAcron#> .  @prefix dul: <http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#> .  @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .  @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .  @prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .  @prefix unit:   <http://datAcron-project.eu/units/> .  @prefix : <http://www.datacron-project.eu/datAcron#> .  @prefix dul: <http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#> . :GapEnd :occurs :node_304655000_1443942005000_-4.5220519999999995_48.337925 .  :node_304655000_1443942005000_-4.5220519999999995_48.337925 a :Node ; :ofMovingObject :ves304655000 ;  :hasHeading \"0.0\"^^unit:degree ;  :hasRateOfTurn \"127.0\" ;  :hasSpeed \"0.0\"^^unit:meterpsec ;  :hasCourse \"15.0\" ;  :hasAvgMessageInterval \"0.0\" ;  :hasNumberOfPoints \"1\" ;  :hasLastDiffTime \"0.0\" ;  :hasMinSpeed \"0.3601108\"^^unit:meterpsec ;  :hasMinDiffTime \"9223372036854775807\" ;  :hasMaxSpeed \"0.3601108\"^^unit:meterpsec ;  :hasMaxDiffTime \"0\" ;  :hasVicinity \"POLYGON((-4.5220519999999995 48.337925, -4.5220519999999995 48.337925, -4.5220519999999995 48.337925, -4.5220519999999995 48.337925, -4.5220519999999995 48.337925))\" ; :hasMinTurn \"127.0\" ;  :hasMaxturn \"127.0\" ;  :hasMinHeading \"209.0\"^^unit:degree ;  :hasMaxHeading��������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������������� \"209.0\"^^unit:degree ;  :hasGeometry :geom__4_5220519999999995_48_337925 ;  :hasSpeed \"0.0\"^^unit:meterpsec ;    :hasTemporalFeature :t1443942005000 . :t1443942005000 a :Instant ;  :TimeStart \"2015-10-04T07:00:05\"^^xsd:DateTime .  :geom__4_5220519999999995_48_337925 a :Point ;  :hasWKT \"POINT (-4.5220519999999995 48.337925)\" .\n:node_0_1443942005000_-4.5220519999999995_48.337925 :hasWeatherCondition :weather_48.337925_-4.5220519999999995_0.0_1443949200 .\n:weather_48.337925_-4.5220519999999995_0.0_1443949200 a :WeatherCondition ;  :windDirectionMin \"287.25284\"^^unit:degree ;  :windDirectionMax \"287.25284\"^^unit:degree ;  :windSpeedMin \"18.240738\"^^unit:meterpsec ;  :windSpeedMax \"18.240738\"^^unit:meterpsec ;  :reportedMinTemperature \"10.5\"^^unit:celcius ;  :reportedMaxTemperature \"10.5\"^^unit:celcius ;  :reportedDewPoint \"5.700012\"^^unit:celcius ;  :reportedPressure \"96891.234\"^^unit:hPa ."

    val parts = e.replaceAll("[;]","").split(":").filterNot(s => s.isEmpty || s == ".")
    val events = e.replaceAll(" ","").split("[:]").filter(annotation.contains(_))

    val vesselInfo = parts.filter(_.contains("node_")).head.split("[_]")
    val info = parts.filter(x => x.contains("hasSpeed") || x.contains("hasHeading")).take(2)

    val id = vesselInfo(1)
    val ts = vesselInfo(2).toLong/1000
    val lon = vesselInfo(3).toDouble
    val lat = vesselInfo(4).replace(" .","").dropRight(1).toDouble
    //println(info.last.replace("hasSpeed ","").replace("^^unit","").replace("\"","").toDouble)
    val speed = info.last.replace("hasSpeed ","").replace("^^unit","").replace("\"","").toDouble*3.6
    val heading = info.head.replace("hasHeading ","").replace("^^unit","").replace("\"","").toDouble

    val parsed = events.map{
      x =>
        val name = annotation(x)
        s"HappensAt [$name $id] $ts"
    }.toVector ++ Vector(s"HappensAt [coord $id $lon $lat] $ts",s"HappensAt [velocity $id $speed $heading] $ts")

    println(heading)
    println(speed)

    println(id)
    println(ts)
    println(lon)
    println(lat)

    println(parsed)*/


    execute(args)

  }

  private def execute(args: Array[String]): Unit = {

    // disable log4j warnings for now
    org.apache.log4j.BasicConfigurator.configure(new NullAppender)

    val inputDir = "./patterns/"
    val outputFile = "./recognition.json"
    // step
    val slidingStep = args.head.toLong
    // working memory
    val windowSize = args(1).toLong
    // dataset specific clock
    val clock = 1L

    // set input directory and output file
    WindowHandler.setIOParameters(inputDir,outputFile)
    // start event recognition loop
    WindowHandler.performER(windowSize,slidingStep,clock)

  }

}

