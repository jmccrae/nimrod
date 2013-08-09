val thresh = opts.doubleValue("thresh","The minimum value to accept a PT entry")
val ptIn = opts.roFile("ptIn[.gz]","The phrase table to read in")
val ptOut = opts.woFile("ptOut[.gz]","The phrase table object to write to")
opts.verify

namedTask("Simple Entropy Filter") {
val out = opts.openOutput(ptOut)

for(line <- opts.openInput(ptIn).getLines) {
  line.split(" \\|\\|\\| ") match {
    case Array(f,t,scores,_,counts) => {
      val Array(pft,lft,ptf,ltf,_) = scores.split(" ")
      val Array(_,_,both) = counts.split(" ")
      val entropy = if(!(f contains " ") && !(t contains " ")) {
        both.toDouble * (math.log(pft.toDouble) + 10) // As suggested by Zens et al.
      } else {
        both.toDouble * (math.log(pft.toDouble) - math.log(lft.toDouble))
      }
      if(entropy > thresh) {
        out.println(line)
      }
    }
    case _ => System.err.println("bad line: " + line)
  }
}
out.flush
out.close
}
