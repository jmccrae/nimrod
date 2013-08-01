package nimrod

object Preprocessor {
  private val discard = "#import ".length

  def apply(builder : StringBuilder) = {
    while(builder.indexOf("#import") >= 0) {
      val start = builder.indexOf("#import")
      val end1 = builder.indexOf("\r",start)
      val end2 = builder.indexOf("\n",start)
      val end = if(end1 >= 0 && end2 >= 0) {
        math.min(end1,end2)
      } else if(end1 < 0 && end2 < 0) {
        builder.size
      } else {
        math.max(end1,end2)
      }
      if(end < start + discard) {
        throw new RuntimeException("Syntax error in #import")
      }
      val pathToLoad = builder.substring(start + "#import ".length, end).replaceAll("^\\s+","").trim()
      println(">>"+pathToLoad+"<<")
      val lines = io.Source.fromFile(pathToLoad).getLines
      builder.delete(start,end)
      val ln = System.getProperty("line.separator")
      var i = start
      for(line <- lines) {
        builder.insert(i,line+ln)
        i += line.length + ln.length
      }
    }
  }
}
