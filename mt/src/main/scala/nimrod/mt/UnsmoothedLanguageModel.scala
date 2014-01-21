import nimrod._

class UnsmoothedLanguageModel(override val args : Seq[String], N : Int) extends Context {
  def name = "Unsmoothed Language Model"

  val corpus = opts.roFile("corpus","The corpus to read from")
  val output = opts.woFile("output","The file to write the langauge model to")

  opts.verify

  val counts = FileArtifact.temporary
  val leftCounts = FileArtifact.temporary

  corpus.lines.mapCombine {
    (no,line) => {
      val tokens = line.split("\\s+").toSeq
      (1 until N) flatMap {
        n => {
          ((n + 1) to math.min(N + n, N)) map {
            m => {
              (tokens.slice(n - 1, m - 1).mkString(" "),1)
            }
          }
        }
      }
    }
  } {
    (x,y) => x + y
  } > counts

  Streamable.fromFile(counts).mapCombine {
    (ngram, count) => {
      val ngramDot = ngram.split(" ").dropRight(1)
      if(!ngramDot.isEmpty) {
        List((ngramDot.mkString(" "), count(0).toInt))
      } else {
        Nil
      }
    }
  } {
    (x, y) => x + y
  } > leftCounts  
}

