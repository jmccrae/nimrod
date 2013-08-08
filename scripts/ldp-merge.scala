if(args.length != 2) {
  System.err.println("Usage:\n\tscala scripts/ldp-merge.scala labels translations < ldp")
  System.exit(-1)
}

val srcIn = io.Source.fromFile(args(0)).getLines
val trgIn = io.Source.fromFile(args(1)).getLines.map(_.replaceAll("@...?$",""))

val translations = (srcIn zip trgIn).toMap

val rdfLiteral = java.util.regex.Pattern.compile("\"([^\"]+)\"" + (if(args.length == 1) { "@" + args(0) } else { "" }))

for(line <- io.Source.stdin.getLines) {
  val toTranslate = collection.mutable.ListBuffer[String]()
  val m = rdfLiteral.matcher(line)
  while(m.find()) {
    toTranslate += m.group(1)
  }
  for(foreign <- toTranslate.toList) {
    line.replace("\""+foreign+"\"","\"" + translations(foreign) + "\"")
  }
}
