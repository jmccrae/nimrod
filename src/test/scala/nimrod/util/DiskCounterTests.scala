package nimrod.util

import org.scalatest._

class DiskCounterTests extends FlatSpec with Matchers  {
/*  val rand = new java.util.Random()

  "disk counter" should "count" in {
    val values : List[Int] = ((0 until 2000) map { x => rand.nextInt(100) }).toList
    val counts = ((0 until 100) map (x => {
      x -> values.filter(_==x).size
    })).toMap
    val counter = new DiskCounter[Int](50)
    for(value <- values) {
      counter.inc(value)
    }
    var read = 0

    System.err.println("Added all values")
    val cvals = counter.values
    System.err.println("Got iterator")
    for((key,value) <- cvals) {
      assert(value == counts(key))
      read += 1
    }
    assert(read == counts.size)
    counter.close
  }*/
}

