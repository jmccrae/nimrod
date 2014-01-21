package nimrod.streams

import nimrod._
import org.scalatest._

class MapStreamableTest extends FlatSpec with Matchers {
  "map streamble" should "put and iterate correctly" in {
    val streamable = new MapStreamable[String, String]()

    streamable.put("a","b")
    streamable.put("e","f")
    streamable.put("c","d")
    streamable.put("a","c")

    val iterator = streamable.iterator
    assert(iterator.next == ("a",List("b","c")))
    assert(iterator.next == ("c",List("d")))
    assert(iterator.next == ("e",List("f")))
    assert(!iterator.hasNext)
    
    streamable.close
  }

  "map streamable" should "map" in {
    val streamable = new MapStreamable[String, String]()

    streamable.put("a","b")
    streamable.put("e","f")
    streamable.put("c","d")

    val newStreamable = streamable.map { (x, y) => Seq((x, y + "z")) }

    val iterator = newStreamable.iterator
    assert(iterator.next == ("a",List("bz")))
    assert(iterator.next == ("c",List("dz")))
    assert(iterator.next == ("e",List("fz")))
    assert(!iterator.hasNext)
    
    newStreamable.close
  }

  "map streamable" should "reduce" in {
    val streamable = new MapStreamable[String, String]()

    streamable.put("a","b")
    streamable.put("e","f")
    streamable.put("c","d")
    streamable.put("a","c")

    val newStreamable = streamable.reduce { (x, ys) => Seq((x + "count", ys.size)) }

    val iterator = newStreamable.iterator
    assert(iterator.next == ("acount",List(2)))
    assert(iterator.next == ("ccount",List(1)))
    assert(iterator.next == ("ecount",List(1)))
    assert(!iterator.hasNext)
    
    newStreamable.close
  }

  "map streamable" should "combine" in {
    val streamable = new MapStreamable[String, String]()

    streamable.put("a","b")
    streamable.put("e","f")
    streamable.put("c","d")
    streamable.put("a","c")

    val newStreamable = streamable.mapCombine { (k,v) => Seq((k,v)) } { _ + _ }

    val iterator = newStreamable.iterator
    assert(iterator.next == ("a",List("bc")))
    assert(iterator.next == ("c",List("d")))
    assert(iterator.next == ("e",List("f")))
    assert(!iterator.hasNext)
    
    newStreamable.close
  }

  "map streamable" should "write to File" in {
    val streamable = new MapStreamable[String, String]()

    streamable.put("a","b")
    streamable.put("e","f")
    streamable.put("c","d")
    streamable.put("a","c")

    val file = java.io.File.createTempFile("test", ".txt")
    file.deleteOnExit()

    implicit val workflow = new Workflow("mapstreamable", "test")

    streamable > FileArtifact(file)

    workflow.start(1)

    val lines = scala.io.Source.fromFile(file).getLines
    
    assert(lines.next == "a\tb\tc")
    assert(lines.next == "c\td")
    assert(lines.next == "e\tf")
    assert(!lines.hasNext)
  }

  "map streamble" should "cogroup" in {
    val streamable1 = new MapStreamable[String, String]()

    streamable1.put("a","b")
    streamable1.put("e","f")
    streamable1.put("c","d")
    streamable1.put("a","c")

    val streamable2 = new MapStreamable[String, Int]()

    streamable2.put("d",1)
    streamable2.put("a",3)
    streamable2.put("d",4)


    val iterator = (streamable1 cogroup streamable2).iterator

    assert(iterator.next == ("a", List((List("b","c"),List(3)))))
    assert(iterator.next == ("c", List((Seq("d"),Nil))))
    assert(iterator.next == ("d", List((Nil,Seq(1,4)))))
    assert(iterator.next == ("e", List((Seq("f"),Nil))))
    assert(!iterator.hasNext)

    streamable1.close
    streamable2.close
  }
}
    

