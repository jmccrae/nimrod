val in = opts.roFile("rdfFile","The input file to extract strings from")
val out = opts.outFileOrStdout()
val lang = opts.string("lang",null,"The language to extract, or omit for untagged strings")
opts.verify

val rdfLiteral = java.util.regex.Pattern.compile("\"([^\"]+)\"" + (if(lang != null) { "@" + lang } else { "" }))

namedTask("Extract tags") {
  for(line <- opts.openInput(in).getLines) {
    val m = rdfLiteral.matcher(line)
    while(m.find()) {
      out.println(m.group(1))
    }
  }
}
