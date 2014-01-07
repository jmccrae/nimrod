if(args.length != 2) {
  System.err.println("Usage: scala basic-filter.scala alignPercent minOcc")
  System.exit(-1)
}

val alignPercent = args(0).toDouble
val minOcc = args(1).toInt

val mosesline = "(.*) \\|\\|\\| (.*) \\|\\|\\| .* \\|\\|\\| (.*) \\|\\|\\| \\d+ \\d+ (\\d+)".r

for(line <- io.Source.stdin.getLines) {
  line match {
    case mosesline(f,t,alignment,bothStr) => {
      if(alignment.split("\\s+").size.toDouble / math.max(f.split("\\s+").size,t.split("\\s+").size) > alignPercent &&
          bothStr.toInt >= minOcc) {
            println(line)
          }
    }
    case _ => System.err.println("bad line: " + line)
  }
}
