import java.lang.{Math => math}
val lex = opts.roFile("LM","The language model")
val out = opts.outFileOrStdout()

opts.verify

namedTask("Remove Zeros") {
  for(line <- io.Source.fromFile(lex).getLines) {
    line.split("\t") match {
      case Array(score1str,word,score2str) => {
        val score1 = score1str.toDouble
        val score2 = score2str.toDouble
        out.println(math.min(0,score1) + "\t" + word + "\t" + score2)
      }
      case Array(score1str,word) => {
        val score1 = score1str.toDouble
        out.println(math.min(0,score1) + "\t" + word)
      }
      case _ => println(line.trim())
    }
  }
}
