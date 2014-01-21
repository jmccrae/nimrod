package nimrod.streams

import nimrod._
import java.io._
import java.lang.Integer
import java.util.TreeMap
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}

/**
 * An implementation of a streamable that using disk-based sort merge, where actions are performed in parallel
 * @param writeInterval The number of elements to hold in memory until writing to disk
 * @param combiner An optional function to applied to map collisions
 * @param ordering The ordering of the keys
 */
class MapStreamable[K, V](writeInterval : Int = 1000000, combiner : Option[(V, V) => V] = None)(implicit ordering : Ordering[K]) extends Streamable[K, V] {
  import MapStreamable._
  // See below for definition of actor
  private val actor = new PutActor(writeInterval, ordering, combiner) 

  /**
   * Insert a single value asynchronously into the map
   */
  def put(k : K, v : V) {
    actor ! new actor.Put(k, v)
  }

  /**
   * Get the iterator (all pending actions will be processed first
   */
  def iterator : Iterator[(K, Seq[V])] = _iterator

  /**
   * Actual implementation of iterator for children
   */
  protected def _iterator = {
    // We make the actor promise to return the result and block until it does
    val p = Promise[Iterator[(K, Seq[V])]]()
    actor ! new actor.Values(p)
    Await.result(p.future, Duration.Inf).asInstanceOf[Iterator[(K, Seq[V])]]
  }

  def close {
    actor.stop
  }

  /**
   * Execute with a cached thread pool. This action will iterate through the map
   * and block until all maps have been completed (but not all inserts!)
   */
  private def withThreadPool(function : (K, Seq[V]) => Unit) {
    val tp = Executors.newCachedThreadPool()
    for((k, vs) <- iterator) {
      tp.execute(new Runnable {
        def run {
          function(k, vs)
        }
      })
    }
    tp.shutdown()
    tp.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)
  }

  def map[K2, V2](by : (K, V) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2](writeInterval)(ordering2) {
    override def iterator = {
      MapStreamable.this.withThreadPool { 
        (k,vs) => {
          for(v <- vs) {
            for((k2,v2) <- by(k,v)) {
              put(k2, v2)
            }
          }
        }
      }
      _iterator
    }
    override def close { MapStreamable.this.close ; super.close }
  }

  def reduce[K2, V2](by : (K, Seq[V]) => Seq[(K2, V2)])(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2](writeInterval)(ordering2) {
    override def iterator = {
      MapStreamable.this.withThreadPool {
        (k,vs) => {
          for((k2, v2) <- by(k, vs)) {
            put(k2, v2)
          }
        }
      }
      _iterator
    }
    override def close { MapStreamable.this.close ; super.close }
  }

  def mapCombine[K2, V2](by : (K, V) => Seq[(K2, V2)])(comb : (V2, V2) => V2)(implicit ordering2 : Ordering[K2]) = new MapStreamable[K2, V2](writeInterval, Some(comb))(ordering2) {
    override def iterator = {
      MapStreamable.this.withThreadPool {
        (k,vs) => {
          for(v <- vs) {
            for((k2, v2) <- by(k, v)) {
              put(k2, v2)
            }
          }
        }
      }
      _iterator
    }
    override def close { MapStreamable.this.close ; super.close }
  }
}

object MapStreamable {
  // Timeout when blocking on iterations
  private val timeoutSeconds = System.getProperty("NIMROD_TIMEOUT","31536000").toInt

  // The basic single threaded iterator
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

  // The  merging iterator. Note iterators cannot be thread-safe!
  private class MapIterator[K, V](files : List[File], ordering : Ordering[K]) extends Iterator[(K, Seq[V])] {
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
    def hasNext = { readers.exists(_.hasNext) }
  }

  // An actor is really just a single thread pool, we use the actor function ! to submit tasks
  trait Actor {
    private val executor = Executors.newSingleThreadExecutor()

    def !(r : Runnable) {
      executor.execute(r)
    }

    def stop {
      executor.shutdown()
    }
  }

  /**
   * The actor responsible for inserting elements into a map streamable
   * @param writeInterval How often to write to disk
   * @param ordering The ordering used for the keys
   * @param combiner The optional function to combine multiple values
   */
  class PutActor[K, V](writeInterval : Int, ordering : Ordering[K], combiner : Option[(V, V) => V]) extends Actor {
    private var theMap = new TreeMap[K, List[V]](ordering)
    var received = 0
    private val syncActor = new SyncActor()

    private def elementsInTheMap = if(combiner == None) {
      received
    } else {
      theMap.size
    }
    
    /**
     * The action to insert a single element in the map
     */
    class Put(key : K, value : V) extends Runnable {
      def run {
        if(received > writeInterval) {
          syncActor ! new syncActor.Sync(theMap, received, None, ordering)
          theMap = new TreeMap[K, List[V]](ordering)
          received = 0
        } 
        theMap.get(key) match {
          case null => theMap.put(key, List(value))
          case s => combiner match {
            case Some(by) => {
              theMap.put(key, List(by(s.head, value)))
            }
            case None => theMap.put(key, value :: s)
          }
        }
        received += 1
      }
    }

    /**
     * The action to get the iterator over values, the result will be returned in the promise
     */
    class Values(p : Promise[Iterator[(K, Seq[V])]]) extends Runnable {
      def run {
        val future = syncActor ! new syncActor.Sync[K, V](theMap, elementsInTheMap, Some(p), ordering)
        theMap = new TreeMap[K, List[V]](ordering)
      }
    }

    override def stop {
      syncActor.stop
      super.stop
    }
  }
  
  /**
   * The actor to write partial maps to disk
   */
  class SyncActor extends Actor {
    private val files = ListBuffer[File]()

    /**
     * Syncs a single map to disk
     */
    class Sync[K, V](map : TreeMap[K, List[V]], size : Int, p : Option[Promise[Iterator[(K, Seq[V])]]], ordering : Ordering[K]) extends
    Runnable {
      def run {
        writeMap(map, size)
        p.map(_.trySuccess(new MapIterator(files.toList, ordering)))
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

