package nimrod.util

import java.io._
import java.lang.Integer
import java.util.TreeMap
import scala.actors._
import scala.collection.JavaConversions._

/**
 * An implementation of a counter that is disk-based and increments (and is thread-safe)
 */
class DiskCounter[A](writeInterval : Int = 1000000)(implicit ordering : Ordering[A]) {
  private var theMap = new TreeMap[A,Integer](ordering)
  private var files : List[File] = Nil
  private case class Inc(key : A)
  private object Values
  private object Close
  private case class Sync(map : TreeMap[A,Integer])
  private val mapLock = new Object
  private val incActor = new Actor {
    def act {
      var received = 0
      var done = false
      while(true) {
        receive {
          case Inc(key) => {
            if(received > writeInterval) {
            //  mapLock.synchronized {
                syncActor !? Sync(theMap)
                theMap = new TreeMap[A,Integer](ordering)
             // }
              received = 0
            } 
           // mapLock.synchronized {
            theMap.get(key) match {
              case null => {
                theMap.put(key,1)
                received += 1
              }
              case s => theMap.put(key,s+1)
            }
            //}
          }
          case Values => {
           // mapLock.synchronized {
            syncActor !? Sync(theMap)
            theMap = new TreeMap[A,Integer](ordering)
            //}
            reply(new ValueIterator(files))
          }
          case Close => {
            done = true
            reply(true)
          }
        }
      }
    }
  }
  incActor.start
  private val syncActor = new Actor {
    def act {
      var done = false
      loop {
        react {
          case Sync(map) => try {
            writeMap(map)
            reply(Unit)
          } catch {
            case x : Exception => x.printStackTrace
          }
          case Close => {
            done = true
            reply(true)
          }
        }
      }
    }
  }
  syncActor.start
  
  private def writeMap(map : TreeMap[A,Integer]) {
    val outFile = File.createTempFile("diskcounter",".bin")
    outFile.deleteOnExit
    val out = new ObjectOutputStream(new FileOutputStream(outFile))
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
    files ::= outFile
  }

  private class SerializedMapIterator(file : File) extends Iterator[(A,Int)] {
    val in = new ObjectInputStream(new FileInputStream(file))
    val _size = in.readInt()
    var read = 0

    def next = {
      read += 1
      (in.readObject().asInstanceOf[A],in.readInt())
    }
    def hasNext = read < _size
  }

  private class ValueIterator(files : List[File]) extends Iterator[(A,Int)] {
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
  
  /** Increment a key (non-blocking) */
  def inc(key : A) = incActor ! Inc(key)
  /** Get all values written so far (this may block) */
  def values : Iterator[(A,Int)] = (incActor !? Values).asInstanceOf[Iterator[(A,Int)]]
  /** Call this to shutdown the actor */
  def close = {
    incActor !? Close
    syncActor !? Close
  }
}
