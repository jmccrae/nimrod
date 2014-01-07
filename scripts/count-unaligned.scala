import org.mapdb.DB

val alignFile = opts.roFile("alignFile","The file containing the alignments")
val trainFile = opts.roFile("trainFile","The training corpus")
val fOutFile = opts.woFile("fOut","The file for foreign counts")
val tOutFile = opts.woFile("tOut","The file for translation counts")

//val cacheSize = System.getProperty("cacheSize","1048576").toInt

opts.verify

//def incConcurrent[K](key : K, map : java.util.concurrent.ConcurrentMap[K,Integer]) {
//  map.get(key) match {
//    case null => { // Key is not yet present
//      if(map.putIfAbsent(key,1) != null) { // Attempt to insert initial value
//        var v = map.get(key) // Didn't work, check map
//        while(!map.replace(key,v,v+1)) { // Try to insert
//          v = map.get(key) // Failed again, reread map
//        }
//      }
//    }
//    case s => { // Key is present
//      if(!map.replace(key,s,s+1)) { // Attempt insert
//        var v = map.get(key) // Failed reread map
//        while(!map.replace(key,v,v+1)) { // Try to insert again
//          v = map.get(key) // Failed again, reread map
//        }
//      }
//    }
//  }
//}

def inc[A](key : A, map : collection.mutable.Map[A,Int]) = map.get(key) match {
  case Some(score) => map.put(key,score+1)
  case None => map.put(key,1)
}

namedTask("Count unaligned") {
  def leftWords(aLine : String)  = aLine split " " map { a => a.substring(0,a.indexOf("-")).toInt }
  def rightWords(aLine : String) = aLine split " " map { a => a.substring(a.indexOf("-")+1).toInt }

//  val db = org.mapdb.DBMaker.newTempFileDB().cacheSize(cacheSize).make() 

  val fCounts = collection.mutable.Map[String,Int]()
  val tCounts = collection.mutable.Map[String,Int]()

  val alignIn = opts.openInput(alignFile).getLines
  val trainIn = opts.openInput(trainFile).getLines

  for((aLine,tLine) <- (alignIn zip trainIn)) {
    val Array(fSent,tSent) = tLine split " \\|\\|\\| "
    val fWords = fSent split " "
    val tWords = tSent split " "
    for(idx <- (0 until fWords.size).toSet -- leftWords(aLine)) {
      inc(fWords(idx),fCounts)
    }
    for(idx <- (0 until tWords.size).toSet -- rightWords(aLine)) {
      inc(tWords(idx),tCounts)
    }
  }

  val fOut = opts.openOutput(fOutFile)
  for((key,value) <- fCounts) {
    fOut.println(key + " " + value)
  }
  fOut.flush
  fOut.close

  val tOut = opts.openOutput(tOutFile)
  for((key,value) <- tCounts) {
    tOut.println(key + " " + value)
  }
  tOut.flush
  tOut.close
}
