val labelFile = opts.roFile("labels","The file containing the labels, which were translated")
val transFile = opts.roFile("translations","The translations of the labels")
val ldpFile = opts.woFile("ldpFile","The source ldpFile")
val out = opts.outFileOrStdout()
opts.verify

namedTask("Update LDP") {
  val srcIn = io.Source.fromFile(labelFile).getLines
  val trgIn = io.Source.fromFile(transFile).getLines.map(_.replaceAll("@...?$",""))

  val translations = (srcIn zip trgIn).toMap

  val rdfLiteral = java.util.regex.Pattern.compile("\"([^\"]+)\"")

  for(line <- opts.openInput(ldpFile).getLines) {
    val toTranslate = collection.mutable.ListBuffer[String]()
    val m = rdfLiteral.matcher(line)
    while(m.find()) {
      toTranslate += m.group(1)
    }
    var newLine = line
    for(foreign <- toTranslate) {
      newLine = newLine.replace("\""+foreign+"\"",translations(foreign))
    }
    out.println(newLine)
  }
  out.flush
  out.close
}
