package nimrod.util

import java.util.concurrent.Executors
import org.scalatest._

class SharedCacheTest extends FlatSpec with Matchers {
  "shared cache" should "function correctly" in {
    var calculated = collection.mutable.Set[Int]()

    val executor = Executors.newFixedThreadPool(10)
    val cache = new SharedCache[Int, Int]()
    for(i <- 1 to 500) {
      val x = util.Random.nextInt(10)
      executor.execute(new Runnable {
        def run {
          cache.get(x, x => {
            if(calculated contains x) {
              throw new RuntimeException()
            }
            calculated += x
            x * x
          })
        }
      })
    }
    executor.shutdown()
  }
}
