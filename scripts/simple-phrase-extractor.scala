import scala.actors.Futures._
import nimrod.util._
val inv = opts.flag("inverse","Use the corpus in inverse mode")
val maxSize = opts.intValue("m","Minimum length",5)
val acceptanceRate = opts.doubleValue("a","The required percentage of alignment",0.75)
val alignFile = opts.roFile("alignFile","The file containing symmetrized alignments")
val corpusFile = opts.roFile("corpus","The corpus file")
val bothFile = opts.woFile("pairs","The file to write the accepted alignments to")
val foreignFile = opts.woFile("foreign","The file to write foreign frequencies to")
val translationFile = opts.woFile("translation","The file to write translation frequencies to")

opts.verify

def alignFromString(astr : String) : Seq[(Int,Int)] = (astr split "\\s+" filter (_ != "") map {
  aelem => aelem split "-" match {
    case Array(a1,a2) => a1.toInt -> a2.toInt
  }
}).toSeq

case class Alignment(val align : Seq[(Int,Int)]) {
  lazy val lMin = left.min
  lazy val lMax = left.max
  lazy val lSize = left.toSet.size
  lazy val rMin = right.min
  lazy val rMax = right.max
  lazy val rSize = right.toSet.size
  lazy val left = align.map(_._1)
  lazy val right = align.map(_._2)

  def lText(words : Array[String]) = {
    words.slice(lMin,lMax+1).mkString(" ")
  }
  def rText(words : Array[String]) = {
    words.slice(rMin,rMax+1).mkString(" ")
  }

  def normalize = Alignment(align map {
    case (l,r) => (l - lMin,r - rMin)
  })
}

def sliceByAlign(strs : Array[String], align : Seq[Int]) = strs.slice(align.min,align.max+1).mkString(" ")

def goodAlign(a : Alignment)  = a.lSize <= maxSize && a.rSize <= maxSize && a.lSize.toDouble / (a.lMax - a.lMin) >= acceptanceRate && a.rSize.toDouble / (a.rMax - a.rMin) >= acceptanceRate
 
namedTask("Simple phrase extraction") {
  val phraseFreq = new util.DiskCounter[(String,String)]()
  val foreignFreq = new util.DiskCounter[String]()
  val transFreq = new util.DiskCounter[String]()

  val alignIn = opts.openInput(alignFile).getLines
  val corpusIn = opts.openInput(corpusFile).getLines

  (alignIn zip corpusIn).toStream.par.foreach {
    case (aline,cline) => {
      val leftAligns = collection.mutable.Set[Seq[Int]]()
      val rightAligns = collection.mutable.Set[Seq[Int]]()
      val clines = cline split " \\|\\|\\| "
      if(clines.size != 2 || (aline matches "\\s+")) {
        System.err.println("Bad line: " + cline)
      } else {
        val fSent = (if(inv) { clines(0) } else { clines(1) }) split " "
        val tSent = (if(inv) { clines(1) } else { clines(0) }) split " "

        val aligns = alignFromString(aline)

        val allAligns = (0 until aligns.size).toStream flatMap ( a => {
          (a+1 until aligns.size).toStream map {
            b => Alignment(aligns.slice(a,b))
          }
        })

        val goodAligns = allAligns filter { goodAlign(_) }

        val texts = goodAligns map { a => (a.lText(fSent),a.rText(tSent)) }

        for(a <- goodAligns) {
          phraseFreq.inc((a.lText(fSent),a.rText(tSent)))
          leftAligns += a.left
          rightAligns += a.right
        }        

        for(a <- leftAligns) {
          foreignFreq.inc(sliceByAlign(fSent,a))
        }

        for(a <- rightAligns) {
          transFreq.inc(sliceByAlign(tSent,a))
        }
      }
    }
  }
  
  { 
    val out = opts.openOutput(bothFile)

    for(((f,t),s) <- phraseFreq.values) {
      out.println("%s ||| %s ||| %d" format (f,t,s))
    }
    out.flush
    out.close
  }
  {
    val out = opts.openOutput(foreignFile)

    for((f,s) <- foreignFreq.values) {
      out.println("%s ||| %d" format (f,s))
    }
    out.flush
    out.close
  }
  {
   val out = opts.openOutput(translationFile)

   for((t,s) <- transFreq.values) {
     out.println("%s ||| %d" format (t,s))
    }
    out.flush
    out.close
  }
  phraseFreq.close
  foreignFreq.close
  transFreq.close
}
