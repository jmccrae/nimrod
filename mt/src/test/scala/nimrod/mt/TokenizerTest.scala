package nimrod.mt

import org.scalatest._

class TokenizerTest extends FlatSpec with Matchers {
  "tokenizer" should "tokenize 4 5 5 6 7 into 5 tokens" in {
    val tokenizer = new StandardRegexTokenizer()
    tokenizer.tokenize("4 5 6 6 7") should be (Array("4","5","6","6","7"))
  }
}
