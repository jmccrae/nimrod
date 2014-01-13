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
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * An implementation of a counter that is disk-based and increments (and is thread-safe)
 */

class MapStreamable[K, V](writeInterval : Int = 1000000)(implicit ordering : Ordering[K]) extends Streamable[K, V] {
  import MapStreamable._
  private val system = ActorSystem("map-streamable" + scala.util.Random.nextInt.toHexString)
  private val actor = system.actorOf(Props(classOf[PutActor[K, V]], writeInterval, ordering))
  implicit private val akkaTimeout = Timeout(Int.MaxValue)

  def put(k : K, v : V) {
    actor ! Put(k, v)
  }

  def iterator : Iterator[(K, Seq[V])] = {
    val future = actor.?(Values)(akkaTimeout)
    Await.result(future, Duration.Inf).asInstanceOf[Iterator[(K, Seq[V])]]
  }

  def close {
    system.shutdown()
  }

  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2](writeInterval)(ordering2) {
    override def iterator = try {
      for((k, vs) <- MapStreamable.this.iterator) {
        for(v <- vs) {
          for((k2, v2) <- by(k, v)) {
            put(k2, v2)
          }
        }
      }
      super.iterator
    } finally {
      close
    }
  }

  def reduce[K2, V2](by : (K, Seq[V]) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2](writeInterval)(ordering2) {
    override def iterator = try {
      for((k,vs) <- MapStreamable.this.iterator) {
        for((k2, v2) <- by(k, vs)) {
          put(k2, v2)
        }
      }
      super.iterator
    } finally {
      close
    }
  }

  def combine[V2](by : (V, V) => V) = new MapStreamable[K, V](writeInterval)(ordering) {
    override def iterator = try {
      for((k, vs) <- MapStreamable.this.iterator) {
        if(!vs.isEmpty) {
          var h = vs.head
          for(v2 <- vs.tail) {
            h = by(h, v2)
          }
          put(k, h)
        }
      } 
      super.iterator
    } finally {
      close
    }
  }

  def >(file : FileArtifact)(implicit workflow : Workflow) : Task = workflow.register(new Task {
    override def exec = {
      val out = file.asStream
      for((k, v) <- iterator) {
        out.println(k.toString + "\t" + v.toString)
      }
      0
    }
    override def messenger = workflow
  })
}

object MapStreamable {
  // messages
  private case class Put[K, V](key : K, value : V)
  private case class Sync[K, V](map : TreeMap[K, List[V]])
  private object Values
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
      val minVal = readers.flatMap(_.peek.map(_._1)).min(ordering)
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

  class PutActor[K, V](writeInterval : Int, ordering : Ordering[K]) extends Actor {
    private var theMap = new TreeMap[K, List[V]](ordering)
    var received = 0
    private val syncActor = context.actorOf(Props(classOf[SyncActor]), "syncActor")
    def receive = {
      case Put(key, value) => {
        if(received > writeInterval) {
          Await.ready(ask(syncActor,Sync(theMap)),Duration.Inf)
          theMap = new TreeMap[K, List[V]](ordering)
          received = 0
        } 
        theMap.get(key) match {
          case null => {
            theMap.put(key.asInstanceOf[K], List(value.asInstanceOf[V]))
            received += 1
          }
          case s => theMap.put(key.asInstanceOf[K], value.asInstanceOf[V] :: s)
        }
      }
      case Values => {
        val future = syncActor ? Sync(theMap)
        val files = Await.result(future, Duration.Inf).asInstanceOf[ListBuffer[File]]
        theMap = new TreeMap[K, List[V]](ordering)
        sender ! new MapIterator(files, ordering)
      }
    }
  }
  
  class SyncActor extends Actor {
    val files = ListBuffer[File]()
    def receive = { 
      case Sync(map) => try {
        writeMap(map)
        sender ! files
      } catch {
        case x : Exception => x.printStackTrace
      }
    }
  
    private def writeMap[K, V](map : TreeMap[K, List[V]]) {
      val outFile = File.createTempFile("diskcounter",".bin")
      outFile.deleteOnExit
      val out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)))
      out.writeInt(map.size)
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



object DiskCounter {
  private case class Inc[A](key : A)
  private object Values
 // private object Close
  private case class Sync[A](map : TreeMap[A,Integer])
  implicit private val akkaTimeout = Timeout(Int.MaxValue)
  private class SerializedMapIterator[A](file : File) extends Iterator[(A,Int)] {
    val in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)))
    val _size = in.readInt()
    var read = 0

    def next = {
      read += 1
      (in.readObject().asInstanceOf[A],in.readInt())
    }
    def hasNext = read < _size
  }

  private class ValueIterator[A](files : ListBuffer[File], ordering : Ordering[A]) extends Iterator[(A,Int)] {
    val readers = files map (file => new PeekableIterator[(A,Int)](new SerializedMapIterator(file)))

    def next = {
      val minVal = readers.flatMap(_.peek.map(_._1)).min(ordering)
      val active = readers filter { 
        _.peek match {
          case Some((v,_)) => minVal == v
          case None => false
        }
      }
      require(!active.isEmpty)
      val sum = active.map(_.peek.get._2).sum
      val key = active.head.peek.get._1
      active.foreach(_.next)
      (key,sum)
    }
    def hasNext = readers.exists(_.hasNext)
  }

  class IncActor[A](writeInterval : Int, ordering : Ordering[A]) extends Actor {
    private var theMap = new TreeMap[A,Integer](ordering)
    var received = 0
    private val syncActor = context.actorOf(Props(classOf[SyncActor]), "syncActor")
    def receive = {
      case Inc(key) => {
        if(received > writeInterval) {
          Await.ready(ask(syncActor,Sync(theMap)),Duration.Inf)
          theMap = new TreeMap[A,Integer](ordering)
          received = 0
        } 
        theMap.get(key) match {
          case null => {
            theMap.put(key.asInstanceOf[A],1)
            received += 1
          }
          case s => theMap.put(key.asInstanceOf[A],s+1)
        }
      }
      case Values => {
        val future = syncActor ? Sync(theMap)
        val files = Await.result(future, Duration.Inf).asInstanceOf[ListBuffer[File]]
        theMap = new TreeMap[A,Integer](ordering)
        sender ! new ValueIterator(files, ordering)
      }
    }
  }
  
  class SyncActor extends Actor {
    val files = ListBuffer[File]()
    def receive = { 
      case Sync(map) => try {
        writeMap(map)
        sender ! files
      } catch {
        case x : Exception => x.printStackTrace
      }
    }
  
    private def writeMap[A](map : TreeMap[A,Integer]) {
      val outFile = File.createTempFile("diskcounter",".bin")
      outFile.deleteOnExit
      val out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)))
      out.writeInt(map.size)
      val entryIterator = map.entrySet().iterator()
      while(entryIterator.hasNext) {
        val entry = entryIterator.next()
        val key = entry.getKey()
        val value = entry.getValue()
        out.writeObject(key)
        out.writeInt(value)
      }
      out.flush
      out.close
      //files ::= outFile
      files.prepend(outFile)
    }
  }



}

class DiskCounter[A](writeInterval : Int = 1000000)(implicit ordering : Ordering[A]) {
  private val system = ActorSystem("disk-counter")
  import DiskCounter._

  private val incActor = system.actorOf(Props(classOf[IncActor[A]], writeInterval, ordering), "incActor")
 
 
  /** Increment a key (non-blocking) */
  def inc(key : A) = incActor ! Inc(key)
  /** Get all values written so far (this may block) */
  def values : Iterator[(A,Int)] = {
    val future = incActor ? Values
    Await.result(future, Duration.Inf).asInstanceOf[Iterator[(A,Int)]]
  }
  /** Call this to shutdown the actor */
  def close = system.shutdown()
}
