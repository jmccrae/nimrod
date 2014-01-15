package nimrod.streams

import nimrod._
import java.io._
import java.lang.Integer
import java.util.TreeMap
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}

/**
 * An implementation of a counter that is disk-based and increments (and is thread-safe)
 */

class MapStreamable[K, V](writeInterval : Int = 1000000, combiner : Option[(V, V) => V] = None)(implicit ordering : Ordering[K]) extends Streamable[K, V] {
  import MapStreamable._
  protected def system : ActorSystem = theSystem
  protected lazy val theSystem = ActorSystem("map-streamable" + scala.util.Random.nextInt.toHexString)
  private val actor = system.actorOf(Props(classOf[PutActor[K, V]], writeInterval, ordering, combiner))
  implicit private val akkaTimeout = Timeout(Int.MaxValue)
  private var forwards = List[ActorRef](actor)

  def put(k : K, v : V) {
    for(a <- forwards) {
      a ! Put(k, v)
    }
  }

  def iterator : Iterator[(K, Seq[V])] = {
    val p = Promise[Iterator[(K, Seq[V])]]()
    actor ! Values(p)
    Await.result(p.future, Duration.Inf).asInstanceOf[Iterator[(K, Seq[V])]]
  }

  def close {
    system.shutdown()
  }

  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2](writeInterval)(ordering2) {
    override def iterator = {
      for((k, vs) <- MapStreamable.this.iterator) {
        for(v <- vs) {
          for((k2, v2) <- by(k, v)) {
            put(k2, v2)
          }
        }
      }
      super.iterator
    }
    override def system = MapStreamable.this.system
  }

  def reduce[K2, V2](by : (K, Seq[V]) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2](writeInterval)(ordering2) {
    override def iterator = {
      for((k,vs) <- MapStreamable.this.iterator) {
        for((k2, v2) <- by(k, vs)) {
          put(k2, v2)
        }
      }
      super.iterator
    }
    override def system = MapStreamable.this.system
  }

  def mapCombine[K2, V2](by : (K, V) => Seq[(K2, V2)])(comb : (V2, V2) => V2)(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2](writeInterval, Some(comb))(ordering2) {
    override def iterator = {
      for((k, vs) <- MapStreamable.this.iterator) {
        for(v <- vs) {
          for((k2, v2) <- by(k, v)) {
            put(k2, v2)
          }
        }
      }
      super.iterator
    }
    override def system = MapStreamable.this.system
  }


  def >(file : FileArtifact, separator : String)(implicit workflow : Workflow) : Task = workflow.register(new Task {
    override def exec = {
      val out = file.asStream
      for((k, v) <- iterator) {
        out.println(k.toString + separator + v.mkString(separator))
      }
      close
      0
    }
    override def messenger = workflow
  })
}

object MapStreamable {
  // messages
  private case class Put[K, V](key : K, value : V)
  private case class Sync[K, V](map : TreeMap[K, List[V]], size : Int, p : Option[Promise[Iterator[(K, Seq[V])]]])
  private case class Values[K, V](p : Promise[Iterator[(K, Seq[V])]])
  private case class IteratorReady[K, V](files : ListBuffer[File], p : Promise[Iterator[(K, Seq[V])]])
  implicit private val akkaTimeout = Timeout(Int.MaxValue)

  private class SerializedMapIterator[K, V](file : File) extends Iterator[(K, V)] {
    val in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)))
    val _size = in.readInt()
    var read = 0

    def next = {
      read += 1
      (in.readObject().asInstanceOf[K],in.readObject().asInstanceOf[V])
    }
    def hasNext = read < _size
  }

  private class MapIterator[K, V](files : ListBuffer[File], ordering : Ordering[K]) extends Iterator[(K, Seq[V])] {
    val readers = files map (file => new PeekableIterator[(K, V)](new SerializedMapIterator(file)))

    def next = {
      val heads = readers.flatMap(_.peek.map(_._1))
      if(heads.isEmpty) {
        throw new NoSuchElementException()
      }
      val minVal = heads.min(ordering)
      var values : List[V] = Nil
      var active = readers filter { 
        _.peek match {
          case Some((v,_)) => minVal == v
          case None => false
        }
      }
      val key = active.head.peek.get._1
      while(!active.isEmpty) {
        values :::= active.map(_.peek.get._2).toList
        active.foreach(_.next)
        active = readers filter { 
          _.peek match {
            case Some((v,_)) => minVal == v
            case None => false
          }
        }
      }
      (key,values)
    }
    def hasNext = readers.exists(_.hasNext)
  }

  private class CombineIterator[K, V](files : ListBuffer[File], ordering : Ordering[K], by : (V, V) => V) extends Iterator[(K, Seq[V])] {
    val readers = files map (file => new PeekableIterator[(K, V)](new SerializedMapIterator(file)))

    def next = {
      val heads = readers.flatMap(_.peek.map(_._1))
      if(heads.isEmpty) {
        throw new NoSuchElementException()
      }
      val minVal = heads.min(ordering)
      var active = readers filter { 
        _.peek match {
          case Some((v,_)) => minVal == v
          case None => false
        }
      }
      val key = active.head.peek.get._1
      var value = active.head.peek.get._2
      active.head.next
      active = active.tail
      while(!active.isEmpty) {
        value = by(value, active.map(_.peek.get._2).reduce(by))
        active.foreach(_.next)
        active = readers filter { 
          _.peek match {
            case Some((v,_)) => minVal == v
            case None => false
          }
        }
      }
      (key,List(value))
    }
    def hasNext = readers.exists(_.hasNext)
  }

  class PutActor[K, V](writeInterval : Int, ordering : Ordering[K], combiner : Option[(V, V) => V]) extends Actor {
    private var theMap = new TreeMap[K, List[V]](ordering)
    var received = 0
    private val syncActor = context.actorOf(Props(classOf[SyncActor]))

    private def elementsInTheMap = if(combiner == None) {
      received
    } else {
      theMap.size
    }

    def receive = {
      case Put(key, value) => {
        if(received > writeInterval) {
          syncActor ! Sync(theMap, received, None)
          theMap = new TreeMap[K, List[V]](ordering)
          received = 0
        } 
        theMap.get(key) match {
          case null => theMap.put(key.asInstanceOf[K], List(value.asInstanceOf[V]))
          case s => combiner match {
            case Some(by) => {
              System.err.println("combining for " + key)
              theMap.put(key.asInstanceOf[K], List(by(s.head, value.asInstanceOf[V])))
            }
            case None => theMap.put(key.asInstanceOf[K], value.asInstanceOf[V] :: s)
          }
        }
        received += 1
      }
      case Values(p) => {
        val future = syncActor ! Sync[K, V](theMap, elementsInTheMap, Some(p.asInstanceOf[Promise[Iterator[(K, Seq[V])]]]))
        theMap = new TreeMap[K, List[V]](ordering)
      }
      case IteratorReady(files, promise) => {
        combiner match {
          case Some(by) => promise.trySuccess(new CombineIterator(files, ordering, by))
          case None => promise.trySuccess(new MapIterator(files, ordering))
        }
      }
    }
  }
  
  class SyncActor extends Actor {
    val files = ListBuffer[File]()
    def receive = { 
      case Sync(map, size, p) => try {
        writeMap(map, size)
        p match {
          case Some(promise) => sender ! IteratorReady(files, promise)
          case None =>
        }
      } catch {
        case x : Exception => x.printStackTrace
      }
    }
  
    private def writeMap[K, V](map : TreeMap[K, List[V]], size : Int) {
      val outFile = File.createTempFile("diskcounter",".bin")
      outFile.deleteOnExit
      val out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)))
      out.writeInt(size)
      val entryIterator = map.entrySet().iterator()
      while(entryIterator.hasNext) {
        val entry = entryIterator.next()
        val key = entry.getKey()
        val values = entry.getValue()
        for(value <- values) {
          out.writeObject(key)
          out.writeObject(value)
        }
      }
      out.flush
      out.close
      files.prepend(outFile)
    }
  }
}

