package nimrod

import org.scalatest._

class ParallelTest extends FlatSpec with matchers.ShouldMatchers {
  "parallel task" should "execute" in {
    implicit val workflow = new Workflow("parallelTest")
    threadPool(3,"test") (i => {
      task {
        println("hello from head " + i)
      }
    })
    println("Starting parallel test")
    workflow.start(1)
  }
}
