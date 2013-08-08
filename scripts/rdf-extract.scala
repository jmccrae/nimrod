val rdfLiteral = java.util.regex.Pattern.compile("\"([^\"]+)\"" + (if(args.length == 1) { "@" + args(0) } else { "" }))

for(line <- io.Source.stdin.getLines) {
  val m = rdfLiteral.matcher(line)
  while(m.find()) {
    println(m.group(1))
  }
}
