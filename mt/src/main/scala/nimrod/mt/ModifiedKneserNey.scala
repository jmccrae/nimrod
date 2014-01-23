package nimrod.mt

import nimrod._

class ModifiedKneserNey(override val args : Seq[String]) extends Context {
  def name = "Modified Kneser-Ney Language Model"

  val corpus = opts.roFile("corpus", "The corpus to train from")
  val N = opts.intValue("N", "The largest n-gram to calculate")
  val output = opts.woFile("output", "The file to write the language model to")

  opts.verify

  val counts = FileArtifact.temporary
  val cocs = FileArtifact.temporary
  val rightDotCounts = FileArtifact.temporary
  val rightDotCoCs = FileArtifact.temporary

  (corpus.lines(monitor % 10000).mapCombine {
    (no, line) => {
      val tokens = line.split("\\s+").toSeq
      (1 until N) flatMap {
        n => {
          ((n + 1) to math.min(n + N, tokens.size)) map {
            m => {
              (tokens.slice(n - 1, m - 1).mkString(" "), 1)
            }
          }
        }
      }
    }
  } {
    (x, y) => x + y
  }) > counts

  Streamable.fromFile(counts, monitor=monitor % 10000).mapCombineOne {
    (ngram, count) => (count.head.toInt, 1)
  } {
    (x, y) => x + y
  } > cocs

  Streamable.fromFile(counts, monitor=monitor % 10000).mapCombine {
    (ngram, count) => {
      val tokens = ngram.split(" ")
      if(tokens.size > 1) {
        (tokens.dropRight(1).mkString(" "), count.head.toInt) :: Nil
      } else {
        Nil
      }
    }
  } {
    (x, y) => x + y
  } > rightDotCounts

  Streamable.fromFile(rightDotCounts, monitor=monitor % 10000).mapCombineOne {
    (ngram, count) => (count.head.toInt, 1)
  } {
    (x, y) => x + y
  } > rightDotCoCs
}
