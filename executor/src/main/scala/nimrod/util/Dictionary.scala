package nimrod.util

import it.unimi.dsi.fastutil.objects._

/**
 * A dictionary is a thread safe map that auto-increments keys
 */
class Dictionary[K](initialKey : Int = 1)(implicit ordering : Ordering[K]) {
  private val map = Object2IntMaps.synchronize(new Object2IntRBTreeMap[K](ordering))
  private var n = initialKey

  def lookup(k : K) : Int = {
    if(map containsKey k) {
      map.get(k)
    } else {
      map.put(k, n)
      n += 1
      n - 1
    }
  }
}
