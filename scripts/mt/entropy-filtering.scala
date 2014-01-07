if(args.length != 2) {
  System.err.println("Usage: scala entropy-filtering.scala thresh phrase-table > new-phrase-table")
  System.exit(-1)
}

val thresh = args(0).toDouble

val mosesLine = "(.*) \\|\\|\\| (.*) \\|\\|\\| ([0-9\\.e-]+) ([0-9\\.e-]+) .* \\|\\|\\| (.*) \\|\\|\\| \\d+ \\d+ (\\d+)".r

val map = collection.mutable.Map[(String,String),Double]()

for(line <- io.Source.fromFile(args(1)).getLines) {
  line match {
    case mosesLine(f,t,prob,_,_,_) => {
      if(f.split("\\s+").size <= 3 && t.split("\\s+").size <= 3) {
        map,put((f,t),prob.toDouble)
      }
    }
  }
}


def lexProb(f : String, t: String) = {
  val fs = f.split("\\s+")
  val ts = t.split("\\t+")

  if(fs.length == 1 || ts.length == 1) {
    return Double.NegativeInfinity
  }
  val scores = for(i <- 1 to fs.size-1) yield {
    for(j <- 1 to ts.size - 1) yield {
      val l1 = (fs.slice(0,i).mkString(" "),ts.slice(0,j).mkString(" "))
      val l2 = (fs.slice(i,fs.size).mkString(" "),ts.slice(j,ts.size).mkString(" "))
      val l3 = (fs.slice(0,i).mkString(" "),ts.slice(j,ts.size).mkString(" "))
      val l4 = (fs.slice(i,fs.size).mkString(" "),ts.slice(0,j).mkString(" "))
      val score1 = map.get(l1) match {
        case Some(s1) => map.get(l2) match {
          case Some(s2) => s1*s2
          case None => Double.NegativeInfinity
        }
          case None => Double.NegativeInfinity
      }
      val score2 = map.get(l3) match {
        case Some(s1) => map.get(l4) match {
          case Some(s2) => s1*s2
          case None => Double.NegativeInfinity
        }
          case None => Double.NegativeInfinity
      }
      math.max(score1,score2)
    }
  }
  scores.flatten.max
}


for(line <- io.Source.fromFile(args(1)).getLines) {
  line match {
    case mosesLine(f,t,prob,lexProb,align,count) => {
      val entropy = count.toDouble * (math.log(prob.toDouble) - math.log(lexProb(f,t))
      if(entropy > thresh) {
        println(line + " ||| " + entropy)
      }
    }
    case _ => System.err.println("bad line: " + line)
  }
}
