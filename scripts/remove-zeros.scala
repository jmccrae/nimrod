import java.lang.{Math => math}
val lex = opts.roFile("LM","The language model")
val lexName = lex.getPath()
val outFile = opts.woFile("out","The langauge model to write to")
val outName = outFile.getPath()

opts.verify

namedTask("Remove Zeros") {
  val out = new java.io.PrintWriter(outName)
  for(line <- io.Source.fromFile(new java.io.File(lexName)).getLines) {
    line.split("\t") match {
      case null => System.err.println("Null line")
      case Array(score1str,word,score2str) => {
        val score1 = score1str.toDouble
        val score2 = score2str.toDouble
        out.println(math.min(0,score1) + "\t" + word + "\t" + score2)
      }
      case Array(score1str,word) => {
        val score1 = score1str.toDouble
        out.println(math.min(0,score1) + "\t" + word)
      }
      case _ => out.println(line.trim())
    }
  }
  out.flush
  out.close
  System.err.println(out.toString)
}
