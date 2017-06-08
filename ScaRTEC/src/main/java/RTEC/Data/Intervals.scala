package RTEC.Data

import scala.util.matching.Regex

object Intervals {
    val empty = Intervals(Vector())

    val intervalPattern = new Regex("\\((\\d+),(inf|\\d+)\\)")
    def fromString(input: String): Intervals = {
        var retval: Vector[(Int, Int)] = Vector()
        for (pair <- intervalPattern findAllIn input) {
            val addition: (Int, Int) = pair match {
                case intervalPattern(x1, "inf") => (x1.toInt, -1)
                case intervalPattern(x1, x2) => (x1.toInt, x2.toInt)
                case error => println(s"$error is not an interval group"); return empty
            }

            retval :+= addition
        }

        Intervals(retval)
    }

    def union(group: Seq[Intervals]): Intervals = {
        if (group.isEmpty) return empty

        var all = group
            .flatMap(_.t)
            .sortWith (_._1 < _._1)

        if (all.isEmpty)
            return Intervals.empty

        var retval = Vector[(Int, Int)]()
        //println(s"Handler.unionIntervals: sorted input = $all")

        var interval = all.head
        if (interval._2 == -1) {
            return Intervals(Vector(interval))
        }

        var reachedInf = false
        all = all.tail
        while (!reachedInf && all.nonEmpty) {
            val head = all.head
            if (head._1 <= interval._2 || interval._2 == -1) {
                interval = (interval._1, if (interval._2 <= head._2) head._2
                                         else if (head._2 != -1) interval._2
                                         else {reachedInf = true; -1})
            } else {
                retval :+= interval
                interval = head
                if (interval._2 == -1) {
                    reachedInf = true
                }
            }
            all = all.tail
        }

        //println(s"Handler.unionIntervals: result = ${retval :+ interval}")
        Intervals(retval :+ interval)
    }

    def intersect(group: Seq[Intervals]): Intervals = {
        if (group exists (_.isEmpty)) {
            return empty
        }
        var filtered = group

        // merged first two of the sequence till there is only one left
        while (filtered.length > 1) {
            filtered = (filtered.head & filtered.tail.head) +: filtered.tail.tail
        }

        filtered.head
    }

    def complement(input: Intervals): Intervals = {
        // Input check
        if (input.isEmpty) return Intervals(Vector((0, -1)))

        // Assuming input is correctly formatted
        var retval = Vector[(Int, Int)]()
        var all = input.t sortWith (_._1 < _._1)

        if (all.head._1 > 0) retval :+= (0, all.head._1)
        while (all.length > 1) {
            retval :+= (all.head._2, all(1)._1)
            all = all.tail
        }
        if ( all.head._2 != -1 ) retval :+= (all.head._2, -1)

        Intervals(retval)
    }

    def fromPoints(input: Set[Int], clock: Int): Intervals = {
        var result = Vector[(Int, Int)]()

        val sorted = input.toVector.sorted
        var (acc, previous) = (sorted.head, sorted.head)
        val tail = sorted.tail
        tail foreach {n =>
            if (n > (previous + clock)) {
                result :+= (acc, previous + clock)
                acc = n
            }
            previous = n
        }
        if (acc != previous)
            result :+= (acc, previous + clock)

        Intervals(result)
    }

    def combine(initiations: Set[Int], terminations: Set[Int], delay: Int): Intervals = {
        // if there is an inconsistency of the dataset, return empty intervals
        if (initiations.intersect(terminations).nonEmpty) return Intervals.empty

        val points: Vector[(Boolean, Int)] =
            (initiations.map(i => (true, i))(collection.breakOut): Vector[(Boolean, Int)]) ++ terminations.map(t => (false, t))(collection.breakOut): Vector[(Boolean, Int)]

        var p = points.sortWith(_._2 < _._2).dropWhile(!_._1)
        if (p.isEmpty)
            return Intervals.empty

        var begin = 0
        var end = 0
        var result = Vector[(Int, Int)]()

        do {
            begin = p.head._2
            p = p.dropWhile(_._1)
            if (p.nonEmpty) {
                end = p.head._2

                result :+= (begin + delay, end + delay)
                p = p.dropWhile(!_._1)

            } else
                result :+= (begin + delay, -1)

        } while (p.nonEmpty)

        Intervals(result)
    }
}

case class Intervals(t: Vector[(Int, Int)]) {
    def isEmpty = t.isEmpty
    def nonEmpty = t.nonEmpty

    override def toString = {
        t.map(interval => s"(${interval._1},${if (interval._2 != -1) interval._2 else "inf"})").mkString(",")
    }

    def head: Int = t.head._1
    def last: Int = t.last._2

    def ==(x: Intervals) = x.t == t

    def contains(x: Intervals): Boolean = {
        val union: Intervals = this | x

        // X should be a subset
        union.t == this.t
    }

    def contains(x: Int): Boolean = {
        t exists(interval => x >= interval._1 && x < interval._2)
    }

    def startPoints: Set[Int] = t.map(_._1)(collection.breakOut)
    def endPoints: Set[Int] = (t.map(_._2)(collection.breakOut): Set[Int]) - -1

    def |(other: Intervals): Intervals = {
        if (this.t.isEmpty)
            return other

        if (other.t.isEmpty)
            return this

        Intervals.union(Seq(this, other))
    }

    def &(other: Intervals): Intervals = {
        if (this.t.isEmpty || other.t.isEmpty)
            return Intervals.empty

        var retval = Vector[(Int, Int)]()
        val first = this.t
        var rest = other.t

        // for each interval of the first sequence get the first n of the second sequence that cross the domain
        // and find the intersection of those
        var matched = Seq[(Int, Int)]()
        for (i <- first if rest.nonEmpty) {
            rest partition (x => (x._2 > i._1 || x._2 == -1) && (x._1 < i._2 || i._2 == -1)) match {
                case (r1, r2) => matched = r1; rest = r2
            }
            //println(s"matched = $matched, s = $s")
            if (matched.nonEmpty) rest +:= matched.last
            retval ++= matched map (x => (i._1.max(x._1), if (i._2 == -1) x._2 else if (x._2 == -1) i._2 else i._2.min(x._2)))
        }
        Intervals(retval)
    }
}
