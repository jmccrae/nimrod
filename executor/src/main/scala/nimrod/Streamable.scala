package nimrod

/**
 * Streamable is a collection that implements map/combine/reduce style data processing
 */
trait Streamable[K, V] {
  /** 
   * Map this data to a new streamable by the given function
   */
  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering : Ordering[K2]) : Streamable[K2, V2]
  /**
   * Map and combine this data to a new streamble by the given function
   */
  def mapCombine[K2, V2](by : (K, V) => Seq[(K2, V2)])(comb : (V2, V2) => V2)(implicit ordering : Ordering[K2]) : Streamable[K2, V2]
  /**
   * Reduce this map the following function
   */
  def reduce[K2, V2](by : (K,Seq[V]) => Seq[(K2, V2)])(implicit ordering : Ordering[K2]) : Streamable[K2, V2]
  /**
   * Create an iterator on this data, this normally executes all maps, combines and reduces further down the chain
   */
  def iterator : Iterator[(K, Seq[V])]
  /**
   * An identifier to the user of what this task does
   */
  def name : String
  /**
   * Save the current state to allow this to be played
   */
  def save()(implicit workflow : Workflow) : SavedStreamable[K, V]


  /**
   * Cogroup this streamable with another streamable
   */
  def cogroup[W](streamable : Streamable[K, W], monitor : ProgressMonitor = NullProgressMonitor)(implicit ordering : Ordering[K]) : Streamable[K, (Seq[V], Seq[W])] = new
  streams.SeqStreamable("CoGroup(" + this.name + "," + streamable.name + ")", new Iterator[(K, (Seq[V], Seq[W]))] {
    lazy val iter1 = new streams.PeekableIterator(iterator)
    lazy val iter2 = new streams.PeekableIterator(streamable.iterator)

    def hasNext = iter1.hasNext || iter2.hasNext

    def next : (K, (Seq[V], Seq[W])) = iter1.peek match {
      case Some((k1, vs1)) => iter2.peek match {
        case Some((k2, vs2)) => {
          val o = ordering.compare(k1,k2)
            if(o < 0) {
              iter1.next
              return (k1, (vs1, Nil))
            } else if(o > 0) {
              iter2.next
              return (k2, (Nil, vs2))
            } else {
              iter1.next
              iter2.next
              return (k1, (vs1, vs2))
            }
          }
          case None => {
            iter1.next
            return (k1, (vs1, Nil))
          }
        }
        case None => {
          if(iter2.hasNext) {
            val (k2, vs2) = iter2.next
            return (k2, (Nil, vs2))
          } else {
            throw new NoSuchElementException()
          }
        }
    }
  }, monitor)

  /**
   * Translate the values only of a streamable (single threaded operation)
   */
  def translate[W](by : V => W, monitor : ProgressMonitor = NullProgressMonitor)(implicit ordering : Ordering[K]) : Streamable[K, W] = new streams.IterStreamable[K,W]("Translate(" + name +
    ")", new Iterator[(K, Seq[W])] {
    lazy val iter = iterator
    def hasNext = iter.hasNext
    def next = iter.next match {
      case (k, vs) => (k, vs.map(by))
    }
  }, monitor)

  /**
   * Dump a map into a file
   * @param file The file to write to
   * @param separator A separator between the string representations of the objects
   * @returns A new task in the current workflow
   */
  def >(file : FileArtifact, separator : String = "\t")(implicit workflow : Workflow) : Task = workflow.register(new Task {
    override def exec = {
      val out = file.asStream
      for((k, v) <- iterator) {
        out.println(k.toString + separator + v.mkString(separator))
      }
      0
    }
    override def toString = name + " > " + file.pathString
    override def messenger = workflow
  })
  /** 
   * Simplified map where the mapper only returns one element
   */
  def mapOne[K2, V2](by : (K, V) => (K2, V2))(implicit ordering : Ordering[K2]) = map { (k, v) => Seq(by(k,v)) }
  /**
   * Simplified map combine where the mapper only returns one element
   */
  def mapCombineOne[K2, V2](by : (K, V) => (K2, V2))(comb : (V2, V2) => V2)(implicit ordering : Ordering[K2]) = mapCombine({ (k, v) =>
    Seq(by(k,v)) })(comb)
  /**
   * Simplified reduce where the reducer only returns one element
   */
  def reduceOne[K2, V2](by : (K, Seq[V]) => (K2, V2))(implicit ordering : Ordering[K2]) = reduce { (k, vs) => Seq(by(k, vs)) }
  /**
   * Combine the streamble with the following function
   */
  def combine(by : (V, V) => V)(implicit ordering : Ordering[K]) = reduce { (k, vs) => Seq((k, vs.reduce(by))) }
  /**
   * Filter this map by the given function
   */
  def filter(by : (K, V) => Boolean)(implicit ordering : Ordering[K]) = map { (k, v) => if(by(k, v)) { Seq((k,v)) } else { Seq() } }
  /**
   * Apply an operation to each element of this map in serial
   */
  def foreach(by : (K, V) => Unit)(implicit workflow : Workflow) : Task = workflow.register(new Task {
    override def exec = {
      for((k, vs) <- iterator) {
        for(v <- vs) {
          by(k, v)
        }
      }
      0
    }
    override def toString = "Application on " + name
    override def messenger = workflow
  })
  /**
   * Convert this to an in-memory(!) map
   */
  def toMap = iterator.toMap
}

object Streamable {
  /** Create a streamable from a sequence */
  def apply[K, V](seq : Seq[(K, V)], monitor : ProgressMonitor = NullProgressMonitor)(implicit ordering : Ordering[K]) : Streamable[K, V] = 
    new streams.SeqStreamable(seq.toString, (seq.sortBy(_._1)).iterator, monitor)
  /** Create a streamable from a file
   * @param artifact The file to read from
   * @param separator The separator between records
   */
  def fromFile(artifact : FileArtifact, separator : String = "\t", monitor : ProgressMonitor = NullProgressMonitor) : Streamable[String, Seq[String]] = 
    new streams.SeqStreamable("file:" + artifact.pathString, artifact.asSource.getLines.flatMap { 
    line => {
      val arr = line.split(separator)
      if(arr.length == 0) {
        None
      } else {
        Some((arr.head,arr.tail.toSeq))
      }
    }
  }, monitor)
  /**
   * Create a streamable whose keys are simply the element (line) number
   */
  def enumerated[V](seq : Iterable[V], monitor : ProgressMonitor = NullProgressMonitor) = new streams.SeqStreamable(seq.toString, (Stream.from(1) zip
    seq).iterator, monitor)
  def enumerated[V](seq : => Iterator[V], monitor : ProgressMonitor) = new streams.SeqStreamable(seq.toString, Stream.from(1).iterator
    zip seq, monitor)
}

trait SavedStreamable[K, V] {
  def apply() : Streamable[K, V]
}
