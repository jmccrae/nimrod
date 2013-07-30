package nimrod.tasks
import org.scalatest._

import nimrod._
import nimrod.tasks._
import java.io._

class gzipTest extends FlatSpec {
  "gzip" should "compress and decompress a file" in {
    val f = File.createTempFile("file",".txt")
    f.deleteOnExit()
    val out = new PrintWriter(f)
    out.println("""Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna
      aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure
      dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident,
      sunt in culpa qui officia deserunt mollit anim id est laborum.""")
    out.flush
    out.close

    implicit val workflow = new Workflow("gzipTests")

    gzip(f)
    gunzip(f+".gz")
    checkExists(f)

    workflow.start

  }
}
