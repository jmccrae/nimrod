package nimrod.util

import scala.collection.mutable.Map
import scala.concurrent._
import scala.concurrent.duration.Duration

/**
 * A thread-safe in memory cache. This can be used to avoid duplicate calculations. If the
 * same key is requested multiple times the system will block each one until necessary
 */
class SharedCache[K, V] {
  private var cache = Map[K, Promise[V]]()

  def get(k : K, calculation : K => V) : V = {
    val (promise, fill : Boolean) = cache.synchronized {
      cache.get(k) match {
        case None => {
          val promise = Promise[V]()
          cache.put(k, promise)
          (promise, true)
        }
        case Some(p) => (p, false)
      }
    }
    if(fill) {
      val v = calculation(k)
      promise.trySuccess(v)
      return v
    } else {
      Await.result(promise.future, Duration.Inf)
    }
  }
}
