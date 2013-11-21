package nimrod.util

import org.mapdb.DB
import scala.collection.JavaConversions._
import java.lang.Integer

/**
 * An implementation of a counter that is disk-based and increments (and is thread-safe)
 */
class DiskCounter[A](sorted : Boolean = false, db : DB = DiskCounter.createDB()) {
  private val name = math.abs(math.random * 1000000000).toString
  private val theMap = if(sorted) {
    db.getTreeMap[A,Integer](name)
  } else {
    db.getHashMap[A,Integer](name)
  }

  def inc(key : A) = {
    // First check if there is a value by trying to put 1
    var v = theMap.putIfAbsent(key,1)
    if(v != null) {
      // Guess there was, let's try to increment this value
      while(!theMap.replace(key,v,v+1)) {
        // Nope, changed again, reread the map
        v = theMap.get(key)
      }
    }
  }
  def get(key : A) : Int = theMap.get(key).intValue
  def values : Iterator[(A,Int)] = theMap.iterator.map {
    case (x,y) => (x,y.intValue)
  }
}

object DiskCounter {
  def createDB(cacheSize : Int = 1048576) = org.mapdb.DBMaker.newTempFileDB().cacheSize(cacheSize).cacheLRUEnable().make()
  def closeDB(db : DB) = db.close
}
