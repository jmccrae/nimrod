val rotIn = opts.roFile("table[.gz]","The (sorted) unmerged reordering table to read")
val ptFile = opts.roFile("pt[.gz]","The (sorted) merged phrase table to use for cofiltering")
val rotOut = opts.woFile("out[.gz]","The target for the merged reordering table")
opts.verify

namedTask("Merge Reordering Table") {

var lastF = ""
var lastT = ""
var scores = Array[Double]()
var n = 0
var linesRead = 0

implicit def sumScores(l1 : Array[Double]) = new {
  def +(l2 : Array[Double]) = (l1 zip l2) map ({
    case (x,y) => x + y
  })
  def /(v : Double) = l1 map (_ / v)
}
val out = opts.openOutput(rotOut)
val pt = opts.openInput(ptFile).getLines

def ftLine(s : String) = s.split(" \\|\\|\\| ") match {
  case Array(x,y,_,_,_) => x + " ||| " + y + " ||| "
  case Array(x,y,_) => x + " ||| " + y + " ||| "
}

var pFT = ftLine(pt.next)
var lastFT = ""
for(line <- opts.openInput(rotIn).getLines) {
  val Array(f,t,s) = line.split(" \\|\\|\\| ")

  if(f != lastF || t != lastT) {
    if(lastF != "" && pFT == lastFT) {
      out.println(lastF + " ||| " + lastT + " ||| " + (scores / n).mkString(" "))
    }
    scores = s.split(" ").map(_.toDouble)
    n = 1
    lastF = f
    lastT = t
    lastFT = ftLine(line)
    while(pFT < lastFT && pt.hasNext) {
      pFT = ftLine(pt.next)
    } 
  } else {
    scores = scores + s.split(" ").map(_.toDouble)
    n += 1
  }
  linesRead += 1
  if(linesRead % 100000 == 0) {
    System.err.print(".")
  }
}
if(lastF != "" && pFT == lastFT) {
  out.println(lastF + " ||| " + lastT + " ||| " + (scores / n).mkString(" "))
}
System.err.println()
out.flush
out.close
}
