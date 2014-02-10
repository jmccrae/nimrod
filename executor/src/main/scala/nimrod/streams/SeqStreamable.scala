package nimrod.streams

import nimrod._

/**
 * Note: Unchecked assumption that the sequence is sorted by key
 */
trait AbstractSeqStreamable[K, V] extends Streamable[K, V] {
  def name : String
  protected def _seq : Iterator[(K,V)]
  def monitor : ProgressMonitor
  private lazy val seq = _seq
  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2]("Map("+name+")", monitor)(ordering2) {
    override def iterator = {
      monitor.reset(0)
      for((k,v) <- seq) {
        for((k2, v2) <- by(k, v)) {
          put(k2,v2)
          monitor.pip
        }
      }
      super.iterator
    }
  }

  def mapCombine[K2, V2](by : (K, V) => Seq[(K2,V2)])(comb : (V2, V2) => V2)(implicit ordering2 : Ordering[K2]) = new
  MapStreamable[K2, V2]("MapCombine("+name+")",monitor,combiner=Some(comb))(ordering2) {
    override def iterator = {
      monitor.reset(0)
      for((k,v) <- seq) {
        for((k2, v2) <- by(k, v)) {
          put(k2,v2)
          monitor.pip
        }
      }
      super.iterator
    }
  }

  def reduce[V2](by : (K, Seq[V]) => Seq[V2])(implicit ordering2 : Ordering[K]) = new MapStreamable[K,
  V2]("Reduce("+name+")",monitor)(ordering2) {
    override def iterator = {
      monitor.reset(0)
      for((k, vs) <- AbstractSeqStreamable.this.iterator) {
        for(v2 <- by(k, vs)) {
          put(k, v2)
          monitor.pip
        }
      }
      super.iterator
    }
  }

  def iterator = new Iterator[(K, Seq[V])] {
    val peekIterator = new PeekableIterator(AbstractSeqStreamable.this.seq)

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

  def save()(implicit workflow : Workflow) = {
    val ss = new SavedStreamable[K, V] {
      def apply() = new SeqStreamable(name, _seq, monitor)
    }
    workflow.register(new Task {
      def run = { iterator ; 0 }
      override def toString = "Save " + name
      override def messenger = workflow
    })
    ss
  }
}

class SeqStreamable[K, V](val name : String, __seq : => Iterator[(K,V)], val monitor : ProgressMonitor = NullProgressMonitor) extends
AbstractSeqStreamable[K, V] {
  protected def _seq = __seq
}

class IterStreamable[K, V](val name : String, _iter : => Iterator[(K, Seq[V])], monitor : ProgressMonitor = NullProgressMonitor)(implicit ordering : Ordering[K]) extends Streamable[K, V] { 
  private lazy val iter = _iter
  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2]("Map("+name+")", monitor)(ordering2) {
    override def iterator = {
      monitor.reset(0)
      for((k,vs) <- iter) {
        for(v <- vs) {
          for((k2, v2) <- by(k, v)) {
            put(k2,v2)
            monitor.pip
          }
        }
      }
      super.iterator
    }
  }

  def mapCombine[K2, V2](by : (K, V) => Seq[(K2,V2)])(comb : (V2, V2) => V2)(implicit ordering2 : Ordering[K2]) = new
  MapStreamable[K2, V2]("MapCombine("+name+")", monitor, combiner=Some(comb))(ordering2) {
    override def iterator = {
      monitor.reset(0)
      for((k,vs) <- iter) {
        for(v <- vs) {
          for((k2, v2) <- by(k, v)) {
            put(k2,v2)
            monitor.pip
          }
        }
      }
      super.iterator
    }
  }

  def reduce[V2](by : (K, Seq[V]) => Seq[V2])(implicit ordering2 : Ordering[K]) = new MapStreamable[K, V2]("Reduce("+name+")",
    monitor)(ordering2) {
    override def iterator = {
      monitor.reset(0)
      for((k, vs) <- iter) {
        for(v2 <- by(k, vs)) {
          put(k, v2)
          monitor.pip
        }
      }
      super.iterator
    }
  }

  def iterator = iter

  def save()(implicit workflow : Workflow) = {
    val ss = new SavedStreamable[K, V] {
      def apply() = new IterStreamable(name, _iter, monitor)(ordering)
    }
    workflow.register(new Task {
      def run = { iterator ; 0 }
      override def toString = "Save " + name
      override def messenger = workflow
    })
    ss
  }
}
