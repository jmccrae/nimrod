package nimrod

trait Streamable[K, V] {
  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering : Ordering[K2]) : Streamable[K2, V2]
  def mapCombine[K2, V2](by : (K, V) => Seq[(K2, V2)])(comb : (V2, V2) => V2)(implicit ordering : Ordering[K2]) : Streamable[K2, V2]
  def reduce[K2, V2](by : (K,Seq[V]) => Seq[(K2, V2)])(implicit ordering : Ordering[K2]) : Streamable[K2, V2]
  def iterator : Iterator[(K, Seq[V])]
  def >(file : FileArtifact, separator : String = "\t")(implicit workflow : Workflow) : Task
}
