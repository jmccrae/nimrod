val whichIsFile = if(args.size == 2 && args(0) == "-lc") {
  1
} else if(args.size == 2 && args(1) == "-lc") {
  0
} else if(args.size == 2) {
  -1
} else {
  0
}

System.err.println(whichIsFile)

if(args.size != 1 && args.size != 2 || whichIsFile < 0) {
  System.err.println("Usage:\n\tscala compare-translation.scala [-lc] reference < translation")
  System.exit(-1)
}


val ref = io.Source.fromFile(args(whichIsFile)).getLines
val trans = io.Source.stdin.getLines
val lc = args.size == 2

for((r,t) <- (ref zip trans)) {
  val tWords = if(lc) {
    t.split("\\s+").map(_.toLowerCase).toSet
  } else {
    t.split("\\s+").toSet
  }
  for(word <- t.split("\\s+")) {
    val word2 = if(lc) {
      word.toLowerCase
    } else {
      word
    }
    if(tWords.contains(word2)) {
      System.out.print("\033[91m" + word + " ")
    } else {
      System.out.print("\033[92m" + word + " ")
    }
  }
  System.out.println("\033[0m ||| " + r)
}

