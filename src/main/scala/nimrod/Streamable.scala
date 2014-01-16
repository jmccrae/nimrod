package nimrod

trait Streamable[K, V] {
  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering : Ordering[K2]) : Streamable[K2, V2]
  def mapCombine[K2, V2](by : (K, V) => Seq[(K2, V2)])(comb : (V2, V2) => V2)(implicit ordering : Ordering[K2]) : Streamable[K2, V2]
  def reduce[K2, V2](by : (K,Seq[V]) => Seq[(K2, V2)])(implicit ordering : Ordering[K2]) : Streamable[K2, V2]
  def iterator : Iterator[(K, Seq[V])]


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
  def mapOne[K2, V2](by : (K, V) => (K2, V2))(implicit ordering : Ordering[K2]) = map { (k, v) => Seq(by(k,v)) }
  def reduceOne[K2, V2](by : (K, Seq[V]) => (K2, V2))(implicit ordering : Ordering[K2]) = reduce { (k, vs) => Seq(by(k, vs)) }
  def combine(by : (V, V) => V)(implicit ordering : Ordering[K]) = reduce { (k, vs) => Seq((k, vs.reduce(by))) }
  def filter(by : (K, V) => Boolean)(implicit ordering : Ordering[K]) = map { (k, v) => if(by(k, v)) { Seq((k,v)) } else { Seq() } }
}

object Streamable {
  def apply[K, V](seq : Seq[(K, V)])(implicit ordering : Ordering[K]) = new streams.SeqStreamable((seq.sortBy(_._1)).iterator)
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
  def enumerated[V](seq : Seq[V]) = new streams.SeqStreamable(((1 to seq.size) zip seq).iterator)
}
