val thresh = opts.doubleValue("thresh","The minimum value to accept a PT entry")
val ptIn = opts.roFile("ptIn[.gz]","The phrase table to read in")
val ptOut = opts.woFile("ptOut[.gz]","The phrase table object to write to")
opts.verify

namedTask("Simple Entropy Filter") {
val out = opts.openOutput(ptOut)

val boost = math.abs(thresh) + 5
val ALIGN_RATE = 0.5

for(line <- opts.openInput(ptIn).getLines) {
  line.split(" \\|\\|\\| ") match {
    case Array(f,t,scores,aligns,counts) => {
      val fTk = f.split(" ").size
      val tTk = t.split(" ").size
      val nAligns = aligns.split(" ").size.toDouble
      val Array(pft,lft,ptf,ltf,_) = scores.split(" ")
      val Array(_,_,both) = counts.split(" ")
      val entropy = if(!(f contains " ")) {
        both.toDouble * (math.log(pft.toDouble) + boost) // As suggested by Zens et al.
      } else {
        both.toDouble * (math.log(pft.toDouble) - math.log(lft.toDouble))
      }
      if(entropy > thresh && nAligns / math.max(fTk,tTk) >= ALIGN_RATE) {
        out.println(line)
      }
    }
    case _ => System.err.println("bad line: " + line)
  }
}
out.flush
out.close
}
