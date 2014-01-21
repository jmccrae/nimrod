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
    override def messenger = workflow
  })
  /** 
   * Simplified map where the mapper only returns one element
   */
  def mapOne[K2, V2](by : (K, V) => (K2, V2))(implicit ordering : Ordering[K2]) = map { (k, v) => Seq(by(k,v)) }
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
}

object Streamable {
  /** Create a streamable from a sequence */
  def apply[K, V](seq : Seq[(K, V)])(implicit ordering : Ordering[K]) = new streams.SeqStreamable((seq.sortBy(_._1)).iterator)
  /** Create a streamable from a file
   * @param artifact The file to read from
   * @param separator The separator between records
   */
  def fromFile(artifact : FileArtifact, separator : String = "\t") = new streams.SeqStreamable(artifact.asSource.getLines.flatMap { 
    line => {
      val arr = line.split(separator)
      if(arr.length == 0) {
        None
      } else {
        Some((arr.head,arr.tail))
      }
    }
  })
  /**
   * Create a streamable whose keys are simply the element (line) number
   */
  def enumerated[V](seq : Seq[V]) = new streams.SeqStreamable(((1 to seq.size) zip seq).iterator)
}
