package RTEC.Execute

import java.io.FileWriter

/**
  * Created by ikon on 22/11/2016.
  */
object Converter {

  def replaceLast(text: String, regex: String, replacement: String): String = {
    text.replaceFirst("(?s)(.*)" + regex, "$1" + replacement)
  }

  def convertDataset(file: String, inputDir: String): Unit = {
    val dataFile = scala.io.Source.fromFile(file).getLines.toArray.filter(y => !y.contains("updateSDE") && y.trim.nonEmpty && !y.contains("%")).map(_.trim)

    val converted = dataFile.map { line =>
      if (line.contains("happensAt")) {
        line.replace("assert( happensAtIE( ", "HappensAt [")
          .replace(") ),", "").replace(") ).", "")
          .replace("(", " ").replace("),", "]")
          .replace(",", "")
      }
      else if (line.contains("holdsFor")) {
        line.replace("assert( holdsForIESI( ", "HoldsFor [")
          .replace(") ),", "").replace(") ).", "")
          .replace(")=", " = ").replace("(", " ")
          .replace(",  ", "] (").replaceFirst(",", "").replace(", ", "-")
      }
      else if (line.contains("holdsAt")) {
        line.replace("assert( holdsAtIE( ", "HoldsAt [").replace(") ),", "")
          .replace(") ).", "").replace("( ", " ").replace(")=", " = ")
          .replace(",", "] ").replace("assert  holdsAtIE ", "HoldsAt [")
          .replace(" ) )]", "").replace(" ) ).", "")
      }
    }

    val w = new FileWriter(s"$inputDir/dataset.txt")
    w.write(converted.mkString("\n"))
    w.close
  }

  // definitions
  def convertPatterns(file: String, inputDir: String): Unit = {
    val oldPatterns = scala.io.Source.fromFile(file).getLines.toArray.filter(y => y.trim.nonEmpty && !y.contains("%")).filterNot(_.contains("+"))
    var preprocessedPatterns = Array.empty[String]
    var isComment = false

    // remove comments
    oldPatterns.foreach { line =>
      if (line.contains("/*")) isComment = true
      else if (line.contains("*/")) isComment = false
      if (!isComment) preprocessedPatterns :+= line
    }

    preprocessedPatterns = preprocessedPatterns.filterNot(_.contains("*")).map(_.replaceAll("\\[]", ",,")).filterNot(_.contains(",,"))


    val newPatterns = preprocessedPatterns.map { old_line =>
      val line = old_line.replaceAll("\\s+$", "")
      var newLine = ""
      if (line.contains("initiatedAt")) {
        newLine = line.replace("initiatedAt(", "InitiatedAt [")
        if (!line.contains("\t") && line.nonEmpty) newLine = "> " + newLine
      }
      else if (line.contains("happensAt(start")) {
        newLine = line.replace("happensAt(start", "HappensAt Start [")
      }
      else if (line.contains("happensAt(end")) {
        newLine = line.replace("happensAt(end", "HappensAt End [")
      }
      else if (line.contains("happensAt")) {
        newLine = line.replace("happensAt(", "HappensAt [")
        if (!line.contains("\t") && line.nonEmpty) newLine = "> " + newLine
      }
      else if (line.contains("holdsFor")) {
        newLine = line.replace("holdsFor(", "HoldsFor [")
        if (!line.contains("\t") && line.nonEmpty) newLine = "> " + newLine
      }
      else if (line.contains("holdsAt")) {
        newLine = line.replace("holdsAt(", "HoldsAt [")
        if (!line.contains("\t") && line.nonEmpty) newLine = "> " + newLine
      }
      else {
        if (line.contains("complement_all")) newLine = line.replace("complement_all", "Complement_All")
        if (line.contains("union_all")) newLine = line.replace("union_all", "Union_All")
        if (line.contains("intersect_all")) newLine = line.replace("intersect_all", "Intersect_All")
        if (line.contains("relative_complement_all")) newLine = line.replace("relative_complement_all", "Relative_Complement_All")
        newLine = newLine.replace("]", "")
      }
      newLine = newLine.replace(" :-", "").replace("(", " ")
        .replace(".", "").replace(",", "").replace(")", "").replace("=", " = ")
      newLine = replaceLast(newLine, " ", "] ")
      newLine
    }
    val w = new FileWriter(s"$inputDir/definitions.txt")
    w.write(newPatterns.mkString("\n"))
    w.close
  }

  def createSpeedLimits(inputDir: String): Unit = {
    val fd = new java.io.FileWriter(s"$inputDir/areas_speed_limits.csv")
    scala.io.Source.fromFile(s"$inputDir/areas.csv").getLines.map(_.split("[|]")(2)).toArray.distinct.foreach{
      area =>
        val upperBound = (22.0 - 8.0) + 1.0
        val r = scala.util.Random
        val limit = 8.0 + r.nextInt(upperBound.toInt)
        fd.write(s"$area|$limit\n")
    }
    fd.close
  }

  def convertToSeconds(in: String): Unit = {
    scala.io.Source.fromFile(in).getLines.foreach{
      line =>
        val parts = line.split(",")
        val ts = parts(1).toLong/1000
        parts(1) = ts.toString
        val newLine = parts.mkString(",")
        val w = new java.io.FileWriter("ttt.csv",true)
        w.write(newLine + "\n")
        w.close
    }
  }

  def convertCombined(in: String): Unit = {
    val annotations = Map(
      0 -> "change_in_speed_end",
      1 -> "change_in_speed_start",
      2 -> "change_in_heading",
      3 -> "gap_end",
      4 -> "slow_motion_end",
      5 -> "slow_motion_start",
      6 -> "stop_end",
      7 -> "stop_start"
    )
    val multiply = 1.852

    var previous = ("","","","","","")
    scala.io.Source.fromFile(in).getLines.foreach{
      line =>
        val w = new java.io.FileWriter("dataset.txt",true)

        if (line.contains("isInArea") || line.contains("leavesArea") || line.contains("proximity")) {

          if (line.contains("isInArea") || line.contains("leavesArea")) {
            val parts = line.split(",")
            val id = parts.head
            val ts = parts(1)
            val areaId = parts(2)
            val name = parts.last

            w.write(s"HappensAt [$name $id $areaId] $ts\n")
            /*if (id == previous._1 && ts == previous._2) {
              w.write(s"$id,$id,$areaId,$ts,$ts,$name,${previous._3},${previous._4},${previous._5},${previous._6}\n")
            }
            else {
              w.write(s"$id,$id,$areaId,$ts,$ts,$name,0.0,0.0,0.0,0.0\n")
            }*/
          }
          else {
            val parts = line.split(",")
            val id1 = parts.head
            val endTime = parts(1)
            val startTime = parts(2)
            val id2 = parts(3)
            val name = parts.last

            //w.write(s"$id1,$id2,none,$startTime,$endTime,$name,0.0,0.0,0.0,0.0\n")
            w.write(s"HoldsFor [$name $id1 $id2 = true] ($startTime-$endTime)\n")

          }

        }
        else {
          val parts = line.split(",")
          val id = parts.head
          val ts = parts(1)
          val lon = parts(2)
          val lat = parts(3)
          val annot = parts(4).toArray
          val speed = new java.math.BigDecimal(parts(5).toDouble * multiply).toPlainString.take(12)
          val heading = parts.last

          // gap_start
          if (annot.length == 1) {
            //previous = (id,ts,lon,lat,speed,heading)
            //w.write(s"$id,$id,none,$ts,$ts,gap_start,$lon,$lat,$speed,$heading\n")
            w.write(s"HappensAt [gap_start $id] $ts\n")
            w.write(s"HappensAt [coord $id $lon $lat] $ts\n")
            w.write(s"HappensAt [velocity $id $speed $heading] $ts\n")
          }
          else {
            var index = 0
            annot.foreach{
              i =>
                if (i.toInt == 49) {
                  previous = (id,ts,lon,lat,speed,heading)
                  val name = annotations(index)
                  w.write(s"HappensAt [$name $id] $ts\n")
                  //w.write(s"$id,$id,none,$ts,$ts,$name,$lon,$lat,$speed,$heading\n")

                }
                index += 1
            }
            w.write(s"HappensAt [coord $id $lon $lat] $ts\n")
            w.write(s"HappensAt [velocity $id $speed $heading] $ts\n")
          }

        }

        w.close
    }
  }

  def convertSynopses(input: String): Unit = {
    val annotations = Map(
      0 -> "change_in_speed_end",
      1 -> "change_in_speed_start",
      2 -> "change_in_heading",
      3 -> "gap_end",
      4 -> "slow_motion_end",
      5 -> "slow_motion_start",
      6 -> "stop_end",
      7 -> "stop_start"
    )
    val multiply = 1.852

    scala.io.Source.fromFile(input).getLines.foreach{
      line =>
        val parts = line.split(",")
        val id = parts.head
        val ts = parts(1).toLong
        val lon = parts(2)
        val lat = parts(3)
        val annot = parts(4).toArray
        val speed = new java.math.BigDecimal(parts(5).toDouble * multiply).toPlainString.take(12)
        val heading = parts.last

        val w = new java.io.FileWriter("synopses.txt",true)

        if (annot.length == 1) {
          w.write(s"HappensAt [gap_start $id] $ts\n")
          w.write(s"HappensAt [coord $id $lon $lat] $ts\n")
          w.write(s"HappensAt [velocity $id $speed $heading] $ts\n")
        }
        else {
          var index = 0
          annot.foreach{
            i =>
              if (i.toInt == 49) {
                val name = annotations(index)
                w.write(s"HappensAt [$name $id] $ts\n")
              }
              index += 1
          }
          w.write(s"HappensAt [coord $id $lon $lat] $ts\n")
          w.write(s"HappensAt [velocity $id $speed $heading] $ts\n")
        }
        w.close
    }

  }

  def convertForCEP(in: String): Unit = {
    val annotations = Map(
      0 -> "change_in_speed_end",
      1 -> "change_in_speed_start",
      2 -> "change_in_heading",
      3 -> "gap_end",
      4 -> "slow_motion_end",
      5 -> "slow_motion_start",
      6 -> "stop_end",
      7 -> "stop_start"
    )
    val multiply = 1.852
    var identifier = ""
    var velocity = "0.0"
    var direction = "0.0"
    var longitude = "0.0"
    var latitude = "0.0"
    var timePoint = ""

    var previous = ("","","","","","")
    scala.io.Source.fromFile(in).getLines.foreach{
      line =>
        val w = new java.io.FileWriter("dataset.csv",true)

        if (line.contains("isInArea") || line.contains("leavesArea") || line.contains("proximity")) {

          if (line.contains("isInArea") || line.contains("leavesArea")) {
            val parts = line.split(",")
            val id = parts.head
            val ts = parts(1)
            val areaId = parts(2)
            val name = parts.last

            //w.write(s"HappensAt [$name $id $areaId] $ts\n")
            if (ts == timePoint && identifier == id) {
              w.write(s"$id,$id,$areaId,$ts,$ts,$name,$longitude,$latitude,$velocity,$direction\n")
            }
            else {
              w.write(s"$id,$id,$areaId,$ts,$ts,$name,0.0,0.0,0.0,0.0\n")
            }

          }
          else {
            val parts = line.split(",")
            val id1 = parts.head
            val endTime = parts(1)
            val startTime = parts(2)
            val id2 = parts(3)
            val name = parts.last

            w.write(s"$id1,$id2,none,$startTime,$endTime,$name,0.0,0.0,0.0,0.0\n")
            //w.write(s"HoldsFor [$name $id1 $id2 = true] ($startTime-$endTime)\n")

          }

        }
        else {
          val parts = line.split(",")
          val id = parts.head
          identifier = id
          val ts = parts(1)
          timePoint = ts
          val lon = parts(2)
          longitude = lon
          val lat = parts(3)
          latitude = lat
          val annot = parts(4).toArray
          val speed = new java.math.BigDecimal(parts(5).toDouble * multiply).toPlainString.take(12)
          velocity = speed
          val heading = parts.last
          direction = heading

          // gap_start
          if (annot.length == 1) {
            //previous = (id,ts,lon,lat,speed,heading)
            w.write(s"$id,$id,none,$ts,$ts,gap_start,$lon,$lat,$speed,$heading\n")
            /*w.write(s"HappensAt [gap_start $id] $ts\n")
            w.write(s"HappensAt [coord $id $lon $lat] $ts\n")
            w.write(s"HappensAt [velocity $id $speed $heading] $ts\n")*/
          }
          else {
            var index = 0
            annot.foreach{
              i =>
                if (i.toInt == 49) {
                  previous = (id,ts,lon,lat,speed,heading)
                  val name = annotations(index)
                  //w.write(s"HappensAt [$name $id] $ts\n")
                  w.write(s"$id,$id,none,$ts,$ts,$name,$lon,$lat,$speed,$heading\n")

                }
                index += 1
            }
            /*w.write(s"HappensAt [coord $id $lon $lat] $ts\n")
            w.write(s"HappensAt [velocity $id $speed $heading] $ts\n")*/
          }

        }

        w.close
    }
  }

}
