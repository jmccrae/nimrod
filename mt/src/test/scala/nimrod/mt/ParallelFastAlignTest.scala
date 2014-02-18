package nimrod.mt

import nimrod._
import org.scalatest._

class ParallelFastAlignTest extends FlatSpec with Matchers {

  "parallel fast align" should "execute" in {
    NimrodEngine.local(new ParallelFastAlign(Seq("mt/src/test/resources/toalign.txt","tmp")))
  }
}
