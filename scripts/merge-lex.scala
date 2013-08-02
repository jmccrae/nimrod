val lex = opts.roFile("lex","The merged lex files")
val N = opts.intValue("N","The number of individual lex files that were merged")
val outFile = opts.woFile("out","Where to write the merged lex files to")
opts.verify

namedTask("Merge Lex") {
  var n = 0
  val probs = new java.util.TreeMap[String,Double]()

  val backoff = 0.01
  var thresh = backoff

  val lexline = "(.* .*) (.*)".r
  for(line <- io.Source.fromFile(lex).getLines) {
    line match {
      case lexline(key,value) => {
        val newVal = Option(probs.get(key)) match {
          case Some(v) => v+value.toDouble / N
          case None => value.toDouble / N
        }
        if(newVal > thresh) {
          probs.put(key,newVal)
        } 
      }
      case _ => System.err.println("bad line: " + line)
    }
  }
  {
    val iter = probs.entrySet().iterator()
    while(iter.hasNext()) {
      if(iter.next().getValue() < thresh) {
        iter.remove()
      }
    }
  }

  val out = new java.io.PrintStream(outFile)

  val iter = probs.entrySet().iterator()
  while(iter.hasNext()) {
    val next = iter.next()
    val key = next.getKey()
    val score = next.getValue()
    out.println(key + " " + (score / n))
  }
  out.flush
  out.close
}
