package nimrod.streams

import nimrod._

/**
 * Note: Unchecked assumption that the sequence is sorted by key
 */
class SeqStreamable[K, V](val name : String, seq : Iterator[(K, V)])(implicit ordering : Ordering[K]) extends Streamable[K, V] {
  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2]("Map("+name+")")(ordering2) {
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
  MapStreamable[K2, V2]("MapCombine("+name+")",combiner=Some(comb))(ordering2) {
    override def iterator = {
      for((k,v) <- seq) {
        for((k2, v2) <- by(k, v)) {
          put(k2,v2)
        }
      }
      super.iterator
    }
  }

  def reduce[K2, V2](by : (K, Seq[V]) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2]("Reduce("+name+")")(ordering2) {
    override def iterator = {
      for((k, vs) <- SeqStreamable.this.iterator) {
        for((k2, v2) <- by(k, vs)) {
          put(k2, v2)
        }
      }
      super.iterator
    }
  }

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

class IterStreamable[K, V](val name : String, iter : Iterator[(K, Seq[V])])(implicit ordering : Ordering[K]) extends Streamable[K, V] { 
  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2]("Map("+name+")")(ordering2) {
    override def iterator = {
      for((k,vs) <- iter) {
        for(v <- vs) {
          for((k2, v2) <- by(k, v)) {
            put(k2,v2)
          }
        }
      }
      super.iterator
    }
  }

  def mapCombine[K2, V2](by : (K, V) => Seq[(K2,V2)])(comb : (V2, V2) => V2)(implicit ordering2 : Ordering[K2]) = new
  MapStreamable[K2, V2]("MapCombine("+name+")",combiner=Some(comb))(ordering2) {
    override def iterator = {
      for((k,vs) <- iter) {
        for(v <- vs) {
          for((k2, v2) <- by(k, v)) {
            put(k2,v2)
          }
        }
      }
      super.iterator
    }
  }

  def reduce[K2, V2](by : (K, Seq[V]) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2]("Reduce("+name+")")(ordering2) {
    override def iterator = {
      for((k, vs) <- iter) {
        for((k2, v2) <- by(k, vs)) {
          put(k2, v2)
        }
      }
      super.iterator
    }
  }

  def iterator = iter
}
