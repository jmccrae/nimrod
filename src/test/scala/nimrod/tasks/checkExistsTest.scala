package nimrod.tasks
import org.scalatest._

import nimrod._
import nimrod.tasks._

class checkExistsTest extends FlatSpec with Matchers {
  "checkExists" should "fail if file does not exist" in {
    implicit val workflow = new Workflow("checkExists","test")

    checkExists("no-such-file")

    evaluating { workflow.start(1) } should produce[RuntimeException]
  }

  "checkExists" should "not fail if the file exists" in {
    implicit val workflow = new Workflow("checkExists","test")

    checkExists("build.sbt")

    workflow.start(1)
  }

}
