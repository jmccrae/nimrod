val l1tmp = opts.string("srcLang","The source language")
val l2tmp = opts.string("trgLang","The target langauge")
val splitSize = opts.intValue("l","The number of lines for each split (<= 0 for no split)",-1)
val clean = !opts.flag("noclean","Do not clean on completion")
opts.restAsSystemProperties
opts.verify

val l1 = List(l1tmp,l2tmp).min
val l2 = List(l1tmp,l2tmp).max
val WORKING = System.getProperty("working",System.getProperty("user.dir") + "/working/" + l1 + "-" + l2)
val heads = System.getProperty("heads","4").toInt
val MOSES_DIR = System.getProperty("mosesDir",System.getProperty("user.home")+"/moses")
val CDEC_DIR = System.getProperty("cdecDir",System.getProperty("user.home")+"/cdec")
val doFilter = System.getProperty("filter","true").toBoolean

mkdir(WORKING).p
mkdir(WORKING).p

gunzip("corpus/corpus-%s-%s.%s.gz" % (l1,l2,l1))
Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l1) < ("corpus/corpus-%s-%s.%s" % (l1,l2,l1)) > (WORKING + "/corpus-%s-%s.tok.%s" % (l1,l2,l1))
rm("corpus/corpus-%s-%s.%s" % (l1,l2,l1))

checkExists(MOSES_DIR+"/truecaser/truecase."+l1)
Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l1) < (WORKING +
  "/corpus-%s-%s.tok.%s" % (l1,l2,l1)) > (WORKING + "/corpus-%s-%s.true.%s" % (l1,l2,l1))

gunzip("corpus/corpus-%s-%s.%s.gz" % (l1,l2,l2))
Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l2) < ("corpus/corpus-%s-%s.%s" % (l1,l2,l2)) > (WORKING + "/corpus-%s-%s.tok.%s" % (l1,l2,l2))
rm("corpus/corpus-%s-%s.%s" % (l1,l2,l2))

checkExists(MOSES_DIR+"/truecaser/truecase."+l2)
Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l2) < (WORKING +
    "/corpus-%s-%s.tok.%s" % (l1,l2,l2)) > (WORKING + "/corpus-%s-%s.true.%s" % (l1,l2,l2))

Do(MOSES_DIR+"/mosesdecoder/scripts/training/clean-corpus-n.perl",
  WORKING + "/corpus-%s-%s.true" % (l1,l2),
  l1,l2,WORKING + "/corpus-%s-%s.clean" % (l1,l2),"1","80")

val WORKING_CORPUS = WORKING + "/corpus-%s-%s" % (l1,l2)

def extractPhrases(WORKING : String, WORKING_CORPUS : String, LM_DIR : String) = {
  mkdir(WORKING + "/model").p

  mkdir(WORKING + "/imodel").p

  Do(CDEC_DIR+"/corpus/paste-files.pl",
      WORKING_CORPUS + "." + l1,
      WORKING_CORPUS + "." + l2) > (WORKING_CORPUS + ".train")

  Do(CDEC_DIR+"/word-aligner/fast_align",
    "-i",WORKING_CORPUS + ".train",
    "-d","-v","-o") > (WORKING + "/%s-%s.fwd_align" % (l1,l2))

  Do(CDEC_DIR+"/word-aligner/fast_align",
    "-i",WORKING_CORPUS + ".train",
    "-d","-v","-o","-r") > (WORKING + "/%s-%s.rev_align" % (l1,l2))

  Do(CDEC_DIR+"/utils/atools",
    "-c","grow-diag-final-and",
    "-i",WORKING + "/%s-%s.fwd_align" % (l1,l2),
    "-j",WORKING + "/%s-%s.rev_align" % (l1,l2)) > (WORKING + "/model/aligned.grow-diag-final-and")

  Do(CDEC_DIR+"/utils/atools",
    "-c","invert",
    "-i",WORKING + "/%s-%s.fwd_align" % (l1,l2)) > (WORKING + "/imodel/fwd_align")

  Do(CDEC_DIR+"/utils/atools",
    "-c","invert",
    "-i",WORKING + "/%s-%s.rev_align" % (l1,l2)) > (WORKING + "/imodel/rev_align")

  Do(CDEC_DIR+"/utils/atools",
    "-c","grow-diag-final-and",
    "-i",WORKING + "/imodel/fwd_align" % (l1,l2),
    "-j",WORKING + "/imodel/rev_align" % (l1,l2)) > (WORKING + "/imodel/aligned.grow-diag-final-and")

  subTask("scripts/simple-phrase-extractor.scala",
    WORKING + "/corpus.train",
    WORKING + "/model/aligned.grow-diag-final-and",
    WORKING + "/model/pair.freqs",
    WORKING + "/model/foreign.freqs",
    WORKING + "/model/translation.freqs")

  subTask("scripts/simple-phrase-extractor.scala",
    "-inverse",
    WORKING + "/corpus.train",
    WORKING + "/imodel/aligned.grow-diag-final-and",
    WORKING + "/imodel/pair.freqs",
    WORKING + "/imodel/foreign.freqs",
    WORKING + "/imodel/translation.freqs")
}

if(splitSize <= 0) {
  extractPhrases(WORKING,WORKING_CORPUS+".clean",WORKING + "/lm/")
} else {
  val nSplits = (wc("corpus/corpus-%s-%s.%s.gz" % (l1,l2,l1)).toDouble / splitSize).ceil.toInt

  split(splitSize,WORKING_CORPUS + ".clean." + l1) { i =>
    WORKING + "/" + i + "/corpus."+l1
  }
  split(splitSize,WORKING_CORPUS + ".clean." + l2) { i =>
    WORKING + "/" + i + "/corpus."+l2
  }

  namedTask("Preparing splits") {
    val l = (WORKING + "/").ls filter (_.matches("\\d+"))
    val groups = if(l.size >= heads) {
      (l grouped l.size/heads).toList
    } else {
      Nil
    }
    val tail = l.size - l.size / heads * heads
    println("===SHARDS===")
    for(i <- 1 to heads) {
      val shards = (groups(i-1) map (WORKING + "/" + _) mkString (" ")) + (
        if(i % heads <= tail && i != heads) {
          " " + WORKING + "/" + l(l.size - (i % heads))
        } else {
          ""
        }
      )
      set("HEAD"+i,shards)
      println("HEAD " + i + ": " + shards)
    }
  }

  threadPool(heads,"Build Translation Model")( i => {
      for(splitWorking <- get("HEAD"+i).split(" ")) {
        extractPhrases(splitWorking, splitWorking + "/corpus", WORKING + "/lm/")
      }
  })

  mkdir(WORKING + "/model").p
  mkdir(WORKING + "/imodel").p

  subTask("scripts/merge-counts.scala",
    ((WORKING + "/model/pair.freqs") +:
    ((1 to heads) map { i => WORKING + "/" + i + "/model/pair.freqs" })):_*) 

  subTask("scripts/merge-counts.scala",
    ((WORKING + "/model/foreign.freqs") +:
    ((1 to heads) map { i => WORKING + "/" + i + "/model/foreign.freqs" })):_*) 

  subTask("scripts/merge-counts.scala",
    ((WORKING + "/model/translation.freqs") +:
    ((1 to heads) map { i => WORKING + "/" + i + "/model/translation.freqs" })):_*) 
      
  if(clean) {
    for(i <- 1 to nSplits) {
      rm(WORKING + "/" + i).ifexists.r
    }
  }
}

if(clean) {
  rm(WORKING + ("/corpus-%s-%s.clean.%s" % (l1,l2,l1))).ifexists
  rm(WORKING + ("/corpus-%s-%s.clean.%s" % (l1,l2,l2))).ifexists
  rm(WORKING + ("/corpus-%s-%s.sb.%s" % (l1,l2,l1))).ifexists
  rm(WORKING + ("/corpus-%s-%s.sb.%s" % (l1,l2,l2))).ifexists
  rm(WORKING + ("/corpus-%s-%s.tok.%s" % (l1,l2,l1))).ifexists
  rm(WORKING + ("/corpus-%s-%s.tok.%s" % (l1,l2,l2))).ifexists
  rm(WORKING + ("/corpus-%s-%s.true.%s" % (l1,l2,l1))).ifexists
  rm(WORKING + ("/corpus-%s-%s.true.%s" % (l1,l2,l2))).ifexists
  rm(WORKING + "/model/aligned-grow-diag-final-and").ifexists
  rm(WORKING + "/imodel/aligned-grow-diag-final-and").ifexists
}
