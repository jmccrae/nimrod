import org.apache.commons.math3.distribution.HypergeometricDistribution
import java.io.{FileInputStream,FileOutputStream,PrintWriter}
import java.util.zip.{GZIPInputStream,GZIPOutputStream}

def fisherExact(cS : Int, cT : Int, cST : Int, N : Int) : Double = {
  val hg = new HypergeometricDistribution(N,math.min(cS,N),math.min(cT,N))
  1.0 - hg.cumulativeProbability(math.min(cST,N))
}

val N = opts.intValue("N","The number of sentences in the corpus")
val thresh = opts.doubleValue("thresh", "The threshold to filter at")
val inFile = opts.roFile("inFile","The phrase table to filter")
val outFile = opts.woFile("outFile","The phrase table to write to")
opts.verify

namedTask("Fisher filter") {
  val mosesLine = "(.*) \\|\\|\\| (.*) \\|\\|\\| .* \\|\\|\\| .* \\|\\|\\| (\\d+) (\\d+) (\\d+)".r

  var lastForeign = ""
  var lastTranslation = ""
  var lineSet = List[String]() 

  val out = if(outFile.getPath() endsWith ".gz") {
    new PrintStream(new GZIPOutputStream(new FileOutputStream(outFile)))
  } else {
    new PrintStream(outFile)
  }
 
  def filterTranslations(lineSet : List[String]) {
    if(!lineSet.isEmpty) {
      val fisherScores = lineSet map ({
        case mosesLine(f,t,first,second,both) => {
          fisherExact(first.toInt+1,second.toInt+1,both.toInt,N)
        }
      })
      if(fisherScores exists (_ < thresh)) {
        for((line,sc) <- (lineSet zip fisherScores) filter (_._2 < thresh)) {
    //      System.err.println(line)
          out.println(line)
        }
      } else {
        val (line,_) = (lineSet zip fisherScores) minBy (_._2)
        out.println(line)
     //   System.err.println(line)
      }
    }
  }

  var linesRead = 0

  val in = if(inFile.getPath() endsWith ".gz") {
    io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(inFile)))
  } else {
    io.Source.fromFile(inFile)
  }

  for(line <- in.getLines) {
    line match {
      case mosesLine(foreign,translation,first,second,both) => {
        if(foreign != lastForeign) {
          if(!lastForeign.contains(" ") && !lastTranslation.contains(" ") && lastForeign != "") {
            filterTranslations(lineSet)
          }
          if(!foreign.contains(" ") && !translation.contains(" ")) {
            lineSet = line :: Nil
          } else {
            lineSet = Nil
          }
        } else {
          if(!foreign.contains(" ") && !translation.contains(" ")) {
            lineSet ::= line
          }
        }
        lastForeign = foreign
        lastTranslation = translation
      } 
      case _ => System.err.println("bad line: " + line)
    }
    linesRead += 1
    if(linesRead % 100000 == 0) {
      System.err.print(".")
    }
  }
  System.err.println()
  out.flush
  out.close

  if(!lastForeign.contains(" ")) {
    filterTranslations(lineSet)
  }
}
