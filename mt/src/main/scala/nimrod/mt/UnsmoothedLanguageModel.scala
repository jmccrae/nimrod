package nimrod.mt

import nimrod._

class UnsmoothedLanguageModel(override val args : Seq[String]) extends Context {
  def name = "Unsmoothed Language Model"

  val corpus = opts.roFile("corpus","The corpus to read from")
  val N = opts.intValue("N", "The largest n-gram to calculate")
  val output = opts.woFile("output","The file to write the language model to")

  opts.verify

  val counts = FileArtifact.temporary
  val leftCounts = FileArtifact.temporary
  val probs = FileArtifact.temporary

  val tokenizer = services.Services.get(classOf[Tokenizer])

  (corpus.lines(monitor).mapCombine {
    (no,line) => {
      val tokens = tokenizer.tokenize(line)
      (1 to N) flatMap {
        n => {
          (0 until tokens.size - n) map {
            m => {
              (tokens.slice(m, m + n).mkString(" "),1)
            }
          }
        }
      }
    }
  } {
    (x,y) => x + y
  } mapOne {
    (ngram, count) => {
      val tokens = ngram.split(" ")
      val ngramDot = tokens.dropRight(1).mkString(" ")
      val lastWord = tokens.last
      (ngramDot, (lastWord, count))
    }
  } reduceOne {
    (ngramDot, counts) => {
      val total : Int = counts.map(_._2).reduce((x,y) => x + y)
      counts.map(c => c match {
        case (word, x) => (word, x.toDouble / total)
      })
    }
  } map {
    (ngramDot, wps) => {
      for(wp <- wps) yield {
        if(ngramDot == "") {
          (wp._1, wp._2)
        } else {
          (ngramDot + " " + wp._1, wp._2)
        }
      }
    }
  }) > probs

  task(new ARPAWriter(Seq(probs.pathString, N.toString, output.pathString)))
}

object UnsmoothedLanguageModel {
  def main(args : Array[String]) {
    NimrodEngine.local(new UnsmoothedLanguageModel(args.toSeq))
  }
}
