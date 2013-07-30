package nimrod.tasks
import org.scalatest._

import nimrod._

class wgetTest extends FlatSpec {
  "downloading a file" should "copy to another file" in {
    implicit val workflow = new Workflow("wgetTests")

    wget("http://www.example.com") > "wgetTest" 

    workflow.start

    workflow.reset

    assert(new java.io.File("wgetTest").delete())
  }
}
