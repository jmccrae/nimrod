package nimrod

import java.io.File
import nimrod.tasks._
import org.scalatest._

class ParallelTest extends FlatSpec with matchers.ShouldMatchers {
  "parallel task" should "execute" in {
    implicit val workflow = new Workflow("parallelTest", "test")
    threadPool(3,"test") (i => {
        mkdir("tmp"+i)
    })
    println("Starting parallel test")
    workflow.start(1)
    for(i <- 1 to 3) {
      assert(new File("tmp" + i).exists)
      new File("tmp" + i).delete
    }
  }
}
