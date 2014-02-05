package nimrod.mt

import nimrod._

class ModifiedKneserNey(override val args : Seq[String]) extends Context {
  def name = "Modified Kneser-Ney Language Model"

  val corpus = opts.roFile("corpus", "The corpus to train from")
  val N = opts.intValue("N", "The largest n-gram to calculate")
  val output = opts.woFile("output", "The file to write the language model to")

  opts.verify

  val tokenizer = services.Services.get(classOf[Tokenizer])
  
  // Streamable[String, Int] => N-gram to token counts
  val counts = (corpus.lines(monitor % 10000).mapCombine {
    (no, line) => {
      val tokens = tokenizer.tokenize(line)
      (1 to N) flatMap {
        n => {
          (0 until tokens.size - n) map {
            m => {
              (tokens.slice(m, m + n).mkString(" "), 1)
            }
          }
        }
      }
    }
  } {
    (x, y) => x + y
  }).save()

  // Streamable[Int, Int] => How many n-grams have a frequency on N
  val cocs = counts().mapCombineOne {
    (ngram, count) => {
      (count, 1)
    }
  } {
    (x, y) => x + y
  }.save()

  // Reduce CoC by removing all values greater than 3 and load into memory
  val cocsUpto3 = cocs().mapCombineOne {
      (count, freq) => if(count >= 3) {
        (3, freq)
      } else {
        (count, freq)
      }
    } {
      (x, y) => x + y
    }.mapToMap(_.head)

  // Calculate magic number, Y
  val Y = result("Calculate Y") {
    cocsUpto3().getOrElse(1,0).toDouble / (cocsUpto3().getOrElse(1,0) + 2 * cocsUpto3().getOrElse(2,0))
  }

  // Calculate magic numbers, D1, D2, D3
  val D = result("Calculate D") {
    val N = cocsUpto3()
    Seq(
      0.0,
      1.0 - 2.0 * Y() * N.getOrElse(2,0) / N.getOrElse(1,1),
      2.0 - 3.0 * Y() * N.getOrElse(3,0) / N.getOrElse(2,1),
      3.0 - 4.0 * Y() * N.getOrElse(4,0) / N.getOrElse(3,1)
    )
  }

  // Diversity of left history
  def diversityOfHistory(n : Int, left : Boolean) = {
    (counts().map {
      (ngram, count) => {
        val tokens = ngram.split(" ")
        if(tokens.size > 1) {
          if(left) {
            (tokens.drop(1).mkString(" "), (tokens.take(1), count)) :: Nil
          } else {
            (tokens.dropRight(1).mkString(" "), (tokens.takeRight(1), count)) :: Nil
          }
        } else {
          Nil
        }
      }
    } reduce {
      (ngram, diversity) => Seq((ngram, diversity.filter(_._2 >= n).toMap.size))
    }).save()
  }


  def N1r = diversityOfHistory(1, false)
  def N2r = diversityOfHistory(2, false)
  def N3r = diversityOfHistory(3, false)
  def N1l = diversityOfHistory(1, true)

  def rightCounts = (counts().mapCombine {
    (ngram, count) => {
      val tokens = ngram.split(" ")
      if(tokens.size > 1) {
        (tokens.dropRight(1).mkString(" "), count) :: Nil
      } else {
        Nil
      }
    }
  } {
    (x, y) => x + y
  }).save()

  val gamma = ((N1r().cogroup(N2r().cogroup(N3r().cogroup(rightCounts())))) reduceOne {
    (ngram : String, values : Seq[(Seq[Int], Seq[(Seq[Int], Seq[(Seq[Int], Seq[Int])])])]) => {
      val n1r = values.head._1.head
      val n2r = values.head._2.head._1.head
      val n3r = values.head._2.head._2.head._1.head
      val count = values.head._2.head._2.head._2.head
      val d = D()
      (ngram, (d(1) * n1r + (d(2) - d(1)) * n2r + (d(3) - d(2)) * n3r).toDouble / count)
    }
  }).save()
      

}

object ModifiedKneserNey {
  def main(args : Array[String]) {
    NimrodEngine.local(new ModifiedKneserNey(args.toSeq))
  }
}
