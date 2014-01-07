import org.mapdb.DB

val shardCount = opts.nonNegIntValue("shards","The number of shards that exist")
opts.verify

val db = org.mapdb.DBMaker.newTempFileDB().cacheSize(cacheSize).make() 

namedTask("Merge phrase table") {
  val fCounts = db.getTreeMap[String,Int]("f")
  val tCounts = db.getTreeMap[String,Int]("t")
  val ftCounts = db.getTreeMap[String,Int]("ft")

  for(shard <- 1 to shardCount) {
    val in = opts.openInput(shard + "/model/phrase-table-filtered.gz")
    for(line <- in) {
      val Array(f,t,_,_,s) = line split " \\|\\|\\| "
      val Array(tStr,fStr,ftStr) = s split " "
      fCounts.get(f) match {
        case null => fCounts.put(f,fStr.toInt)
        case score => fCounts.put(f,fStr.toInt + score)
      }
      tCounts.get(t) match {
        case null => fCounts.put(t,tStr.toInt)
        case score => fCounts.put(t,tStr.toInt + score)
      }
      ftCounts.get(f + " ||| " + t) match {
        case null => fCounts.put(ft,ftStr.toInt)
        case score => fCounts.put(ft,ftStr.toInt + score)
      }
    }
    val lexIn = opts.openInput(shard + "/model/lex.e2f")
    for(line <- lexIn) {
      val Array(f,t,s) = line split " "
      if(f == "NULL" || t == "NULL") {
        ftCounts
  }

  // p(f | t) = c(ft) / c(t)
  def pft(f : String, t : String) = {
    ftCounts.get(f + " ||| " + t) match {
      case null => 0.0
      case ftScr => ftScr.toDouble / tCounts.get(t) 
    }
  }

  def ptf(f : String, t : String) = {
    ftCounts.get(f + " ||| " + t) match {
      case null => 0.0
      case ftScr => ftScr.toDouble / fCounts.get(t) 
    }
  }

  def lft(aligns : List[(String,String)], unaligned : List[String]) = {

}
