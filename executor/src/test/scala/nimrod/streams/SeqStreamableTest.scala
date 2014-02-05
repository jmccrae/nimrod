package nimrod.streams

import nimrod._
import org.scalatest._

class SeqStreamableTest extends FlatSpec with Matchers {
  implicit val o = new Ordering[(String, List[String])] {
    def compare(x1 : (String,List[String]), x2 : (String, List[String])) = x1._1.compareTo(x2._1)
  }

  "seq streamble" should "put and iterate correctly" in {
    val streamable = new SeqStreamable[String, String]("test",List(("a","b"),("e","f"),("c","d"),("a","c")).sorted.iterator)

    val iterator = streamable.iterator
    assert(iterator.next == ("a",List("c","b")))
    assert(iterator.next == ("c",List("d")))
    assert(iterator.next == ("e",List("f")))
    assert(!iterator.hasNext)
    
  }

  "seq streamable" should "map" in {
    val streamable = new SeqStreamable[String, String]("test",List(("a","b"),("e","f"),("c","d")).sorted.iterator)

    val newStreamable = streamable.map { (x, y) => Seq((x, y + "z")) }

    val iterator = newStreamable.iterator
    assert(iterator.next == ("a",List("bz")))
    assert(iterator.next == ("c",List("dz")))
    assert(iterator.next == ("e",List("fz")))
    assert(!iterator.hasNext)
    
  }

  "seq streamable" should "reduce" in {
    val streamable = new SeqStreamable[String, String]("test",List(("a","b"),("e","f"),("c","d"),("a","c")).sorted.iterator)

    val newStreamable = streamable.reduce { (x, ys) => Seq((x + "count", ys.size)) }

    val iterator = newStreamable.iterator
    assert(iterator.next == ("acount",List(2)))
    assert(iterator.next == ("ccount",List(1)))
    assert(iterator.next == ("ecount",List(1)))
    assert(!iterator.hasNext)
    
  }

  "seq streamable" should "combine" in {
    val streamable = new SeqStreamable[String, String]("test",List(("a","b"),("e","f"),("c","d"),("a","c")).sorted.iterator)

    val newStreamable = streamable.mapCombine { (k,v) => Seq((k,v)) } { _ + _ }

    val iterator = newStreamable.iterator
    assert(iterator.next == ("a",List("bc")))
    assert(iterator.next == ("c",List("d")))
    assert(iterator.next == ("e",List("f")))
    assert(!iterator.hasNext)
    
  }
  "iter streamble" should "put and iterate correctly" in {
    val streamable = new IterStreamable[String, String]("test",List(("a",List("b","c")),("e",List("f")),("c",List("d"))).sorted.iterator)

    val iterator = streamable.iterator
    assert(iterator.next == ("a",List("b","c")))
    assert(iterator.next == ("c",List("d")))
    assert(iterator.next == ("e",List("f")))
    assert(!iterator.hasNext)
    
  }

  "iter streamable" should "map" in {
    val streamable = new IterStreamable[String, String]("test",List(("a",List("b")),("e",List("f")),("c",List("d"))).sorted.iterator)

    val newStreamable = streamable.map { (x, y) => Seq((x, y + "z")) }

    val iterator = newStreamable.iterator
    assert(iterator.next == ("a",List("bz")))
    assert(iterator.next == ("c",List("dz")))
    assert(iterator.next == ("e",List("fz")))
    assert(!iterator.hasNext)
    
  }

  "iter streamable" should "reduce" in {
    val streamable = new IterStreamable[String, String]("test",List(("a",List("b","c")),("e",List("f")),("c",List("d"))).sorted.iterator)

    val newStreamable = streamable.reduce { (x, ys) => Seq((x + "count", ys.size)) }

    val iterator = newStreamable.iterator
    assert(iterator.next == ("acount",List(2)))
    assert(iterator.next == ("ccount",List(1)))
    assert(iterator.next == ("ecount",List(1)))
    assert(!iterator.hasNext)
    
  }

  "iter streamable" should "combine" in {
    val streamable = new IterStreamable[String, String]("test",List(("a",List("b","c")),("e",List("f")),("c",List("d"))).sorted.iterator)

    val newStreamable = streamable.mapCombine { (k,v) => Seq((k,v)) } { _ + _ }

    val iterator = newStreamable.iterator
    assert(iterator.next == ("a",List("bc")))
    assert(iterator.next == ("c",List("d")))
    assert(iterator.next == ("e",List("f")))
    assert(!iterator.hasNext)
  }

  "seq streamable" should "save" in {
    val streamable = new SeqStreamable[String, String]("test",List(("a","b"),("e","f"),("c","d"),("a","c")).sorted.iterator)

    val saved = streamable.save()(new Workflow("test","test"))

    for(i <- 1 to 2) {
      val iterator = saved().iterator
      assert(iterator.next == ("a",List("c","b")))
      assert(iterator.next == ("c",List("d")))
      assert(iterator.next == ("e",List("f")))
      assert(!iterator.hasNext)
    }
  }
}
    

