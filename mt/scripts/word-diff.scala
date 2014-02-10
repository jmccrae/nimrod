val in = opts.roFile("in", "The list of words")
val outFile = opts.woFile("out", "The output")

opts.verify

val counter = new streams.MapStreamable[String,Int]("counter", combiner=Some({ (x,y) => x + y }))
val out = outFile.asStream

def getTransform(from : String, to : String) = {
  val common = (from zip to).takeWhile({ case (x,y) => x == y }).map({ case(x,y) => x }).mkString("")
  from.substring(common.size) + " => " + to.substring(common.size)
}

var words = 0

task("Compile words") {
  var lastWord = ""

  for(line <- in.asSource.getLines) {
    val word = line.split("\t")(0)
    counter.put(getTransform(lastWord, word), 1)
    lastWord = word
    words += 1
  }
  err.println("words=%d" format (words))

}

var possCache = new util.SharedCache[String, Int]()

def endingFreq(l : String) = {
  var poss = 0
  for(line <- in.asSource.getLines) {
    if(line.split("\t")(0).endsWith(l)) {
      poss += 1
    }
  }
  poss
}

val scores = (counter reduce {
  (transform, counts) => {
    val Array(l, r) = if(transform.endsWith(" => ")) {
      Array(transform.dropRight(4),"")
    } else {
      transform.split(" => ")
    }
    val count = counts.sum
    if(count > 10) {
      val pl = possCache.get(l, endingFreq)
      val pr = possCache.get(r, endingFreq)

      println("count=%d p(%s)=%d p(%s)=%d" format (count, l, pl, r, pr))
      (count.toDouble * math.log(count.toDouble * words / pl / pr)) :: Nil
    } else {
      Nil
    }
  }
}).save()


scores() foreach { 
  (k, v) => out.println("%.8f\t%s" format (v, k))
}
