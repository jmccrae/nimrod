import scala.io._

if(args.length == 0 || args.length > 2) {
  System.err.println("Usage:\n\tscala word-error-rate.scala [-lc] ref < translation")
  System.exit(-1)
}
val lc = args.length == 2 && (args(0) == "-lc" || args(1) == "-lc")
val fname = if(args.length == 2 && args(0) == "-lc") {
  args(1)
} else {
  args(0)
}
val ref = io.Source.fromFile(fname).getLines

def levenshteinDist(s1 : List[String], s2 : List[String]) = {
  val distCache = collection.mutable.Map[(Int,Int),Int]()
  def ld(s1 : List[String], s2 : List[String]) : Int = (s1,s2) match {
    case (Nil,s2) => s2.size
    case (s1,Nil) => s1.size
    case (t1 :: ss1, t2 :: ss2) => {
      distCache.get((s1.size,s2.size)) match {
        case Some(x) => x
        case None => {
          val cost = if(t1 == t2) { 0 } else { 1 }
          val dist = math.min(ld(ss1,ss2),
            math.min(ld(t1 :: ss1,ss2),
              ld(ss1,t2 :: ss2))) + cost
          distCache.put((s1.size,s2.size), dist)
          dist
        }
      }
    }
  }
  ld(s1,s2)
}

var sumDist = 0.0
var n = 0

for((trans,ref) <- (ref zip io.Source.stdin.getLines)) {
  val s1 = if(lc) {
    trans.toLowerCase.split(" ").toList
  } else {
    trans.split(" ").toList
  }
  val s2 = if(lc) {
    ref.toLowerCase.split(" ").toList
  } else {
    ref.split(" ").toList
  }
  val dist = levenshteinDist(s1,s2)
  sumDist += dist
  n += 1
}

println("Word Error Rate: " + (sumDist / n))
