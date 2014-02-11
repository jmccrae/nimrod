package nimrod.mt

import org.scalatest._

class FastAlignTest extends FlatSpec with Matchers {
  "fast align" should "produce result as C++ version" in {
    val result = FastAlign.fast_align("mt/src/test/resources/toalign.txt")

    result.printAlign

    result.error should be (None)
    result.likelihood should be (-20.07 +- 0.01)
    result.base2_likelihood should be (-28.96 +- 0.01)
    result.cross_entropy should be (1.93 +- 0.01)
    result.perplexity should be (3.81 +- 0.01)
    result.posterior_p0 should be (0.0)
    result.posterior_al_feat should be (0.0)
    result.size_counts should be (3)
  }
}
