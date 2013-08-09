val rotIn = opts.roFile("table[.gz]","The unmerged reordering table to read")
val rotOut = opts.woFile("out[.gz]","The target for the merged reordering table")
opts.verify

val out = opts.openOutput(rotOut)

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

for(line <- opts.openInput(rotIn).getLines) {
  val Array(f,t,s) = line.split(" \\|\\|\\| ")
  if(f != lastF && t != lastT) {
    if(lastF != "") {
      out.println(lastF + " ||| " + lastT + " ||| " + (scores / n).mkString(" "))
    }
    scores = s.split(" ").map(_.toDouble)
    n = 1
    lastF = f
    lastT = t
  } else {
    scores = scores + s.split(" ").map(_.toDouble)
    n += 1
  }
  linesRead += 1
  if(linesRead % 100000 == 0) {
    System.err.print(".")
  }
}
System.err.println()
out.flush
out.close
