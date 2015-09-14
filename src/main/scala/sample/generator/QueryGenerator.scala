package sample.generator

import java.sql.Timestamp
import java.util.Calendar
import org.scalacheck.Gen._
import scala.collection.mutable.Set

object QueryGenerator {

  val jobGen = {
    var prevLogTime = Timestamp.valueOf("2014-09-12 20:00:00")
    // val logOffsetGen = frequency(
    //   (60, 0),
    //   (40, choose(60 * 1000, 10 * 60 * 1000))
    // )
    val usedStartSet = Set[Timestamp]()
    def addMillis(t: Timestamp, m: Int): Timestamp = {
      val cal = Calendar.getInstance()
      cal.setTime(t)
      cal.add(Calendar.MILLISECOND, m)
      new Timestamp(cal.getTimeInMillis())
    }

    def zeroMillis(t: Timestamp): Timestamp = {
      val cal = Calendar.getInstance()
      cal.setTime(t)
      cal.set(Calendar.MILLISECOND, 0)
      new Timestamp(cal.getTimeInMillis())
    }

    for {
      // logOffset <- logOffsetGen
      logOffset <- choose(60 * 100, 3 * 60 * 1000)
      log <- addMillis(prevLogTime, logOffset)
      endOffset <- choose(1000, 600000)
      end <- zeroMillis(addMillis(prevLogTime, -endOffset))
      queueOffset <- choose(1000, 60 * 60 * 12 * 1000)
      queue <- zeroMillis(addMillis(end, -queueOffset))
      startOffset <- choose(1000, 5 * 60 * 1000)
        .suchThat(x => !usedStartSet.contains(zeroMillis(addMillis(queue, -x))))
      start <- zeroMillis(addMillis(queue, -startOffset))
      startEpoch <- start.getTime() / 1000
      masterNum <- choose(1, 10)
      clientNum <- choose(1, 20)
    } yield (
      {
        prevLogTime = log
        usedStartSet.add(start)
        val master = "m" + masterNum
        val client = "cl" + "_" + master + "_" + clientNum
        (start, queue, end, log, startEpoch, master, client)
      }
    )
  }

  val jobListGen = for {
    n <- choose(0, 10)
    l <- listOfN(n, jobGen)
  } yield l

}
