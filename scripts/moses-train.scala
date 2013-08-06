val l1tmp = opts.string("srcLang","The source language")
val l2tmp = opts.string("trgLang","The target langauge")
val splitSize = opts.intValue("l","The number of lines for each split (<= 0 for no split)",-1)
val resume = opts.flag("resume","Resume based on previous state")
opts.restAsSystemProperties
opts.verify

val l1 = List(l1tmp,l2tmp).min
val l2 = List(l1tmp,l2tmp).max
val WORKING = System.getProperty("user.dir") + "/working/" + l1 + "-" + l2
val heads = 4
val MOSES_DIR = System.getProperty("mosesDir","/home/jmccrae/moses")
val CDEC_DIR = System.getProperty("cdecDir","/home/jmccrae/cdec")

//export IRSTLM=`pwd`/irstlm

if(!resume) {
mkdir(WORKING).p
mkdir(WORKING).p

//block("Prepare corpus " + l1) {
  gunzip("corpus/corpus-%s-%s.%s.gz" % (l1,l2,l1))
  Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l1) < ("corpus/corpus-%s-%s.%s" % (l1,l2,l1)) > ("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l1))
  checkExists(MOSES_DIR+"/truecaser/truecase."+l1)
  Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l1) < ("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l1)) > ("corpus/corpus-%s-%s.true.%s" % (l1,l2,l1))
//}

//block("Prepare corpus " + l2) {
  gunzip("corpus/corpus-%s-%s.%s.gz" % (l1,l2,l2))
  Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l2) < ("corpus/corpus-%s-%s.%s" % (l1,l2,l2)) > ("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l2))
  checkExists(MOSES_DIR+"/truecaser/truecase."+l2)
  Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l2) < ("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l2)) > ("corpus/corpus-%s-%s.true.%s" % (l1,l2,l2))
//}

//block("Clean corpus ") {
  Do(MOSES_DIR+"/mosesdecoder/scripts/training/clean-corpus-n.perl",
    "corpus/corpus-%s-%s.true" % (l1,l2),
    l1,l2,WORKING + "/corpus-%s-%s.clean" % (l1,l2),"1","80")
//}
}
val WORKING_CORPUS = WORKING + "/corpus-%s-%s" % (l1,l2)


def buildLM(lm : String) {
  if(!resume || !new File(WORKING + "/../lm/"+lm).exists) {
    mkdir(WORKING + "/../lm").p

    Do(MOSES_DIR+"/irstlm/bin/add-start-end.sh") < (WORKING_CORPUS + ".clean." + lm) > (WORKING_CORPUS + ".sb." + lm)

    rm(WORKING + "/../lm/" + lm).ifexists

    Do(MOSES_DIR+"/irstlm/bin/build-lm.sh",
      "-i",WORKING_CORPUS + ".sb." + lm,
      "-t","tmp",
      "-p","-s","improved-kneser-ney",
      "-o",WORKING + "/../lm/" + lm).env("IRSTLM",MOSES_DIR+"/irstlm")

    Do (MOSES_DIR+"/irstlm/bin/compile-lm",
      "--text","yes",
      WORKING + "/../lm/"+lm+".gz",
      WORKING + "/../lm/"+lm)
  }
  //}
}  

buildLM(l1)
buildLM(l2)

def buildTranslationModel(WORKING : String, WORKING_CORPUS : String, LM_DIR : String) = {
  if(!resume || !new File(WORKING+"/imodel/phrase-table-filtered.gz").exists) {
  mkdir(WORKING + "/model").p

  mkdir(WORKING + "/imodel").p

//block("Alignment") {
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
  //}

  //block("Phrase table generation") {
    val lmFile1 = new File(LM_DIR+l1).getCanonicalPath()
    Do(MOSES_DIR+"/mosesdecoder/scripts/training/train-model.perl",
      "-do-steps","4-9","-root-dir",WORKING,
      "-corpus",WORKING_CORPUS,
      "-f",l1,"-e",l2,
      "-alignment","grow-diag-final-and",
      "-reordering","msd-bidirectional-fe",
      "-lm","0:3:"+lmFile1+":8",
      "-external-bin-dir",MOSES_DIR + "/tools")
    val lmFile2 = new File(LM_DIR+l2).getCanonicalPath()
    Do(MOSES_DIR+"/mosesdecoder/scripts/training/train-model.perl",
      "-do-steps","4-9","-root-dir",WORKING,
      "-corpus",WORKING_CORPUS,
      "-f",l2,"-e",l1,
      "-alignment","grow-diag-final-and",
      "-reordering","msd-bidirectional-fe",
      "-lm","0:3:"+lmFile2+":8",
      "-model-dir",WORKING + "/imodel",
      "-external-bin-dir",MOSES_DIR + "/tools")

    val N = if(splitSize <= 0) {
      500000
    } else {
      splitSize
    }

    subTask("scripts/fisher-filter.scala",N.toString,"0.5",
      WORKING+"/model/phrase-table.gz",WORKING+"/model/phrase-table-filtered.gz")

    subTask("scripts/fisher-filter.scala",N.toString,"0.5",
      WORKING+"/imodel/phrase-table.gz",WORKING+"/imodel/phrase-table-filtered.gz")
  //}
  }
}

if(splitSize <= 0) {
  buildTranslationModel(WORKING,WORKING_CORPUS+".clean",WORKING + "/../lm/")
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
    val groups = (l grouped l.size/heads).toList
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
        buildTranslationModel(splitWorking, splitWorking + "/corpus", WORKING + "/../lm/")
      }
  })

  mkdir(WORKING + "/model").p
  mkdir(WORKING + "/imodel").p

  cat(find(WORKING)(file => {
    file.getPath() endsWith "/model/phrase-table-filtered.gz"
  }).apply) > (WORKING + "/model/phrase-table-all")

  sort(WORKING + "/model/phrase-table-all") > (WORKING + "/model/phrase-table-sorted")

  gzip(WORKING + "/model/phrase-table-sorted")

  cat(find(WORKING)(file => {
    file.getPath() endsWith "/imodel/phrase-table-filtered.gz"
  }).apply) > (WORKING + "/imodel/phrase-table-all")

  sort(WORKING + "/imodel/phrase-table-all") > (WORKING + "/imodel/phrase-table-sorted")

  gzip(WORKING + "/imodel/phrase-table-sorted")

  cat(find(WORKING)(file => {
    file.getPath() endsWith "/model/lex.e2f"
  }).apply) > (WORKING + "/model/lex.e2f.tmp")

  cat(find(WORKING)(file => {
    file.getPath() endsWith "/model/lex.f2e"
  }).apply) > (WORKING + "/model/lex.f2e.tmp")

  cat(find(WORKING)(file => {
    file.getPath() endsWith "/imodel/lex.e2f"
  }).apply) > (WORKING + "/imodel/lex.e2f.tmp")

  cat(find(WORKING)(file => {
    file.getPath() endsWith "/imodel/lex.f2e"
  }).apply) > (WORKING + "/imodel/lex.f2e.tmp")

  subTask("scripts/merge-lex.scala",WORKING + "/model/lex.e2f.tmp", nSplits.toString, WORKING + "/model/lex.e2f")

  subTask("scripts/merge-lex.scala",WORKING + "/model/lex.f2e.tmp", nSplits.toString, WORKING + "/model/lex.f2e")

  subTask("scripts/merge-lex.scala",WORKING + "/imodel/lex.e2f.tmp", nSplits.toString, WORKING + "/imodel/lex.e2f")

  subTask("scripts/merge-lex.scala",WORKING + "/imodel/lex.f2e.tmp", nSplits.toString, WORKING + "/imodel/lex.f2e")

  subTask("scripts/merge-pts.scala",
    WORKING + "/model/phrase-table-sorted",
    WORKING + "/model/lex.e2f",
    WORKING + "/model/lex.f2e",
    WORKING + "/model/phrase-table")

  subTask("scripts/merge-pts.scala",
    WORKING + "/imodel/phrase-table-sorted",
    WORKING + "/imodel/lex.e2f",
    WORKING + "/imodel/lex.f2e",
    WORKING + "/imodel/phrase-table")
}
