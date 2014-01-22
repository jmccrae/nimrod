package nimrod.mt

import nimrod._

class UnsmoothedLanguageModel(override val args : Seq[String]) extends Context {
  def name = "Unsmoothed Language Model"

  val corpus = opts.roFile("corpus","The corpus to read from")
  val N = opts.intValue("N", "The largest n-gram to calculate")
  val output = opts.woFile("output","The file to write the langauge model to")

  opts.verify

  val counts = FileArtifact.temporary
  val leftCounts = FileArtifact.temporary
  val probs = FileArtifact.temporary

  (corpus.lines.mapCombine {
    (no,line) => {
      val tokens = line.split("\\s+").toSeq
      (1 until N) flatMap {
        n => {
          ((n + 1) to math.min(n + N, tokens.size)) map {
            m => {
              (tokens.slice(n - 1, m - 1).mkString(" "),1)
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
  } reduce {
    (ngramDot, counts) => {
      val total : Int = counts.map(_._2).reduce((x,y) => x + y)
      counts.map(c => c match {
        case (word, x) => (ngramDot, (word, x.toDouble / total))
      })
    }
  } mapOne {
    (ngramDot, wp) => {
      if(ngramDot == "") {
        (wp._1, wp._2)
      } else {
        (ngramDot + " " + wp._1, wp._2)
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
