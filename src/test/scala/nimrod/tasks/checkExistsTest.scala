package nimrod.tasks
import org.scalatest._

import nimrod._
import nimrod.tasks._

class checkExistsTest extends FlatSpec with matchers.ShouldMatchers {
  "checkExists" should "fail if file does not exist" in {
    implicit val workflow = new Workflow("checkExists")

    checkExists("no-such-file")

    evaluating { workflow.start } should produce[RuntimeException]
  }

  "checkExists" should "not fail if the file exists" in {
    implicit val workflow = new Workflow("checkExists")

    checkExists("build.sbt")

    workflow.start
  }

}
