package nimrod.streams

import org.scalatest._

class PeekableIteratorTest extends FlatSpec with Matchers {
  "PeekableIterator" should "peek correctly" in {
    val elems = 1 to 5
    val iter = new PeekableIterator(elems.iterator)
    assert(iter.peek == Some(1))
    assert(iter.peek == Some(1))
    assert(iter.next == 1)
    assert(iter.next == 2)
    assert(iter.peek == Some(3))
    assert(iter.peek == Some(3))
    iter.next
    iter.next
    iter.next
    assert(!iter.hasNext)
  }
}
