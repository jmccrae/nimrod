package nimrod.mt

import nimrod._

class ExtractVocabulary(override val args : Seq[String]) extends Context {
  def name = "Extract vocabulary"

  val lc = opts.flag("lc", "Lowercase the words")
  val letters = opts.flag("L", "Include only words consisting of Unicode letters")
  val corpus = opts.roFile("corpus", "The corpus to read from")
  val outFile = opts.woFile("output", "The file to write the word list to")

  opts.verify

  val out = outFile.asStream

  val tokenizer = services.Services.get(classOf[Tokenizer])

  corpus.lines(monitor % 10000).mapCombine {
    (no, line) => {
      val tokens = tokenizer.tokenize(line)
      for(token <- tokens if !letters || token.matches("\\p{L}+")) yield {
        if(lc) {
          (token.toLowerCase(), 1)
        } else {
          (token, 1)
        }
      }
    }
  } {
    (x, y) => (x + y)
  } foreach {
    (word, count) => out.println(word + "\t" + count)
  }
}

object ExtractVocabulary {
  def main(args : Array[String]) {
    System.in.read()
    NimrodEngine.local(new ExtractVocabulary(args.toSeq))
  }
}
