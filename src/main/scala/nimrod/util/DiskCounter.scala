package nimrod.util

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
