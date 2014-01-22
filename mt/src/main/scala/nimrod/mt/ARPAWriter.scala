package nimrod.mt

import nimrod._
import scala.math.log10

class ARPAWriter(val args : Seq[String]) extends Context {
  def name = "ARPA Writer"

  val probs = opts.roFile("probs", "The probabilities as calculated")
  val N = opts.intValue("N", "The largest n-gram to calculate")
  val lmFile = opts.woFile("out", "The file to write the output probabilities")

  opts.verify

  val totals = result("Calculate Totals") {
    val x = ((Streamable.fromFile(probs)).mapCombineOne({
      (k, v) => {
        (k.split(" ").size, 1)
      }
    })({
      (x, y) => x + y
    })).toMap.mapValues {
      case Seq(v) => v
    }
    x
  }
    
  val out = lmFile.asStream

  task("Print headers") {
    out.println("\\data\\")
    for(i <- 1 to N) {
      out.println("ngram %d=%d" format (i, totals().apply(i)))
    }
    out.println()
  }

  for(i <- 1 to N) {
    task("Start %s-grams" format(i)) {
      out.println("\\%d-grams:" format (i))
    }
    Streamable.fromFile(probs, monitor=monitor).filter(
      (k, v) => {
        k.split(" ").size == i
      }
    ).foreach(
      (k, vs) => if(vs.size != 1 && vs.size != 2) {
        throw new RuntimeException("No or too many probabilities")
      } else if(vs.size == 1) {
        out.println("%.8f\t%s" format (log10(vs.head.toDouble), k))
      } else if(vs.size == 2) {
        out.println("%.8f\t%s\t%.8f" format (log10(vs.head.toDouble), k, log10(vs.tail.head.toDouble)))
      }
    )
    task("End %s-grams" format(i)) {
      out.println()
    }
  }

  task("Print footers") {
    out.println("\\end\\")
    out.flush
    out.close
  }
}
