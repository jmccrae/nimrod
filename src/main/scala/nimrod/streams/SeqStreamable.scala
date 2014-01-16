package nimrod.streams

import nimrod._

/**
 * Note: Unchecked assumption that the sequence is sorted by key
 */
class SeqStreamable[K, V](seq : Iterator[(K, V)])(implicit ordering : Ordering[K]) extends Streamable[K,V] {

  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2]()(ordering2) {
    override def iterator = {
      for((k,v) <- seq) {
        for((k2, v2) <- by(k, v)) {
          put(k2,v2)
        }
      }
      super.iterator
    }
  }

  def mapCombine[K2, V2](by : (K, V) => Seq[(K2,V2)])(comb : (V2, V2) => V2)(implicit ordering2 : Ordering[K2]) = new
  MapStreamable[K2, V2](combiner=Some(comb))(ordering2) {
    override def iterator = {
      for((k,v) <- seq) {
        for((k2, v2) <- by(k, v)) {
          put(k2,v2)
        }
      }
      super.iterator
    }
  }

  def reduce[K2, V2](by : (K, Seq[V]) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = map((k, v) => Seq((k, v))).reduce(by)

  def iterator = new Iterator[(K, Seq[V])] {
    val peekIterator = new PeekableIterator(SeqStreamable.this.seq)

    def next = peekIterator.peek match {
      case Some((k,v)) => {
        var vs = v :: Nil
        peekIterator.next
        while(peekIterator.peek.filter(_._1 == k) != None) {
          vs ::= peekIterator.next._2
        }
        (k,vs)
      }
      case None => throw new NoSuchElementException
    }

    def hasNext = peekIterator.hasNext
  }
}

