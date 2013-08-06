val foreignFile = opts.roFile("foreign","The foreign language corpus")
val translationFile = opts.roFile("translation","The translation language corpus")
val sim = opts.roFile("sim","The similarity file")
val N = opts.intValue("N","The top N documents to select")
val foreignOut = opts.woFile("fout","The file to write the foreign language subcorpus to")
val translationOut = opts.woFile("tout","The file to write the translation language subcorpus to")
opts.verify

case class Ranking(val lineNo : Int, score : Double) 

implicit val rankingOrdering = new java.util.Comparator[Ranking] { 
  def compare(x : Ranking, y : Ranking) = {
    x.score.compareTo(y.score) match {
      case 0 => x.lineNo.compareTo(y.lineNo)
      case z => z
    }
  }
}

val rankings = new java.util.TreeSet[Ranking]()

val simIn = opts.openInput(sim).getLines
simIn.next // discard header line

for(line <- simIn) {
  line.split(",") match {
    case Array(n,_,score) => {
      val r = Ranking(n.toInt,score.toDouble)
      if(!rankings.isEmpty || rankingOrdering.compare(r,rankings.first) > 0) {
        rankings.add(r)
        if(rankings.size > N) {
          rankings.remove(rankings.first)
        }
      }
    }
  }
}

import scala.collection.JavaConversions._

var lines = rankings.map(_.lineNo)

val fIn = opts.openInput(foreignFile).getLines
val tIn = opts.openInput(translationFile).getLines
val fOut = opts.openOutput(foreignOut)
val tOut = opts.openOutput(translationOut)

var linesRead = 0

for((fLine,tLine) <- (fIn zip tIn)) {
  if(lines contains linesRead) {
    fOut.println(fLine)
    tOut.println(tLine)
  }
}
fOut.flush
fOut.close
tOut.flush
tOut.close

