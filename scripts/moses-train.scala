val l1tmp = opts.string("srcLang","The source language")
val l2tmp = opts.string("trgLang","The target langauge")
val splitSize = opts.intValue("l","The number of lines for each split (<= 0 for no split)",-1)
opts.verify

val l1 = List(l1tmp,l2tmp).min
val l2 = List(l1tmp,l2tmp).max
val WORKING = System.getProperty("user.dir") + "/working/" + l1 + "-" + l2
val heads = 4
val MOSES_DIR = System.getProperty("mosesDir","/home/jmccrae/moses")
val CDEC_DIR = System.getProperty("cdecDir","/home/jmccrae/cdec")

//export IRSTLM=`pwd`/irstlm

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

val WORKING_CORPUS = WORKING + "/corpus-%s-%s" % (l1,l2)

def buildLM(lm : String) = {
 // block("Build language model for " + lm) {
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
  //}
}  

buildLM(l1)
buildLM(l2)

def buildTranslationModel(WORKING : String, WORKING_CORPUS : String, LM_DIR : String) = {
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

if(splitSize <= 0) {
  buildTranslationModel(WORKING,WORKING_CORPUS+".clean",WORKING + "/../lm/")
} else {

  split(splitSize,WORKING_CORPUS + ".clean." + l1) { i =>
    WORKING + "/" + i + "/corpus."+l1
  }
  split(splitSize,WORKING_CORPUS + ".clean." + l2) { i =>
    WORKING + "/" + i + "/corpus."+l2
  }

  task {
    val l = (WORKING + "/").ls filter (_.matches("\\d+"))
    val groups = (l grouped (l.size / heads)).toList
    for(i <- 1 to heads) {
      set("HEAD"+i,groups(i-1) map (WORKING + "/" + _) mkString (" "))
    }
  }

  threadPool(3,"Build Translation Model")( i => {
      for(splitWorking <- get("HEAD"+i).split(" ")) {
        buildTranslationModel(splitWorking, splitWorking + "/corpus", WORKING + "/../lm/")
      }
  })
}
