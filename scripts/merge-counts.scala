val outFile = opts.woFile("outFile","The file to write the merged counts to")
val args = opts.listOfArgs("files","The list of files to merge")
opts.verify

namedTask("Merge counts") {
  val readers = args.map { arg => new util.PeekableIterator(opts.openInput(arg).getLines) }
  val out = opts.openOutput(outFile)

  while(readers.exists(_.hasNext)) {
    val lines = readers.flatMap(_.peek)
    val keyCounts = lines.map( line => {
      (line.substring(0,line.lastIndexOf("|||") + 4), line.substring(line.lastIndexOf("|||")+4).toInt)
    })
    val minKey = keyCounts.map(_._1).min
    val total = keyCounts.filter(_._1==minKey).map(_._2).sum
    val active = readers.filter { 
      reader => {
        reader.peek match {
          case Some(line) => line.startsWith(minKey)
          case None => false
        }
      }
    }
    active.foreach(_.next)
    out.println(minKey + total)
  }
  out.flush
  out.close
}
