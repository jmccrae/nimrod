val l1tmp = opts.string("srcLang","The source language")
val l2tmp = opts.string("trgLang","The target langauge")
val splitSize = opts.intValue("l","The number of lines for each split (<= 0 for no split)",-1)
val resume = opts.flag("resume","Resume based on previous state")
val mert = !opts.flag("nomert","Do not tune using MERT")
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

if(!resume) {
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
}
val WORKING_CORPUS = WORKING + "/corpus-%s-%s" % (l1,l2)


def buildLM(lm : String) {
  if(!resume || !new File(WORKING + "/lm/"+lm).exists) {
    mkdir(WORKING + "/lm").p

    Do(MOSES_DIR+"/irstlm/bin/add-start-end.sh") < (WORKING_CORPUS + ".clean." + lm) > (WORKING_CORPUS + ".sb." + lm)

    rm(WORKING + "/lm/" + lm + ".tmp").ifexists

    Do(MOSES_DIR+"/irstlm/bin/build-lm.sh",
      "-i",WORKING_CORPUS + ".sb." + lm,
      "-t","lm_tmp_directory",
      "-p","-s","improved-kneser-ney",
      "-o",WORKING + "/lm/" + lm+".tmp").env("IRSTLM",MOSES_DIR+"/irstlm")

    Do (MOSES_DIR+"/irstlm/bin/compile-lm",
      "--text","yes",
      WORKING + "/lm/"+lm+".tmp.gz",
      WORKING + "/lm/"+lm+".tmp")

    subTask("scripts/remove-zeros.scala",
      WORKING + "/lm/"+lm+".tmp",
      WORKING + "/lm/"+lm)

    Do(MOSES_DIR+"/mosesdecoder/bin/build_binary",
      WORKING + "/lm/"+lm,
      WORKING + "/lm/"+lm+".bin")

    if(clean) {
      rm(WORKING + "/lm/" + lm + ".tmp").ifexists
      rm(WORKING + "/lm/" + lm + ".tmp.gz").ifexists
    }
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

    if(doFilter) {
      subTask("scripts/mt/simple-entropy.scala","20",
        WORKING+"/model/phrase-table.gz",WORKING+"/model/phrase-table-filtered.gz")

      subTask("scripts/mt/simple-entropy.scala","20",
        WORKING+"/imodel/phrase-table.gz",WORKING+"/imodel/phrase-table-filtered.gz")
    }
  }
}

if(splitSize <= 0) {
  buildTranslationModel(WORKING,WORKING_CORPUS+".clean",WORKING + "/lm/")
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
        buildTranslationModel(splitWorking, splitWorking + "/corpus", WORKING + "/lm/")
      }
  })

  mkdir(WORKING + "/model").p
  mkdir(WORKING + "/imodel").p

  cat(find(WORKING)(file => {
    if(doFilter) {
      file.getPath() endsWith "/model/phrase-table-filtered.gz"
    } else {
      file.getPath() endsWith "/model/phrase-table.gz"
    }
  }).apply) > (WORKING + "/model/phrase-table-all")

  sort(WORKING + "/model/phrase-table-all") > (WORKING + "/model/phrase-table-sorted")

  gzip(WORKING + "/model/phrase-table-sorted")

  cat(find(WORKING)(file => {
    if(doFilter) {
      file.getPath() endsWith "/imodel/phrase-table-filtered.gz"
    } else {
      file.getPath() endsWith "/imodel/phrase-table.gz"
    }

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

  cat(find(WORKING)(file => {
    file.getPath() endsWith "/model/reordering-table.wbe-msd-bidirectional-fe.gz"
  }).apply) > (WORKING + "/model/reordering-table")

  cat(find(WORKING)(file => {
    file.getPath() endsWith "/imodel/reordering-table.wbe-msd-bidirectional-fe.gz"
  }).apply) > (WORKING + "/imodel/reordering-table")

  sort(WORKING + "/model/reordering-table") > (WORKING + "/model/reordering-table.sorted")

  sort(WORKING + "/imodel/reordering-table") > (WORKING + "/imodel/reordering-table.sorted")

  subTask("scripts/merge-lex.scala",WORKING + "/model/lex.e2f.tmp", nSplits.toString, WORKING + "/model/lex.e2f")

  subTask("scripts/merge-lex.scala",WORKING + "/model/lex.f2e.tmp", nSplits.toString, WORKING + "/model/lex.f2e")

  subTask("scripts/merge-lex.scala",WORKING + "/imodel/lex.e2f.tmp", nSplits.toString, WORKING + "/imodel/lex.e2f")

  subTask("scripts/merge-lex.scala",WORKING + "/imodel/lex.f2e.tmp", nSplits.toString, WORKING + "/imodel/lex.f2e")

  subTask("scripts/merge-pts.scala",
    WORKING + "/model/phrase-table-sorted",
    WORKING + "/model/lex.e2f",
    WORKING + "/model/lex.f2e",
    WORKING + "/model/phrase-table-filtered.gz")

  subTask("scripts/merge-pts.scala",
    WORKING + "/imodel/phrase-table-sorted",
    WORKING + "/imodel/lex.e2f",
    WORKING + "/imodel/lex.f2e",
    WORKING + "/imodel/phrase-table-filtered.gz")

  subTask("scripts/merge-rot.scala",
    WORKING + "/model/reordering-table.sorted",
    WORKING + "/model/phrase-table-filtered.gz",
    WORKING + "/model/reordering-table.wbe-msd-bidirectional-fe.gz")

  subTask("scripts/merge-rot.scala",
    WORKING + "/imodel/reordering-table.sorted",
    WORKING + "/imodel/phrase-table-filtered.gz",
    WORKING + "/imodel/reordering-table.wbe-msd-bidirectional-fe.gz")

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
  rm(WORKING + "/model/extract.inv.sorted.gz").ifexists
  rm(WORKING + "/model/extract.o.sorted.gz").ifexists
  rm(WORKING + "/model/extract.sorted.gz").ifexists
  rm(WORKING + "/model/lex.e2f").ifexists
  rm(WORKING + "/model/lex.e2f.tmp").ifexists
  rm(WORKING + "/model/lex.f2e").ifexists
  rm(WORKING + "/model/lex.f2e.tmp").ifexists
  rm(WORKING + "/model/phrase-table-all").ifexists
  rm(WORKING + "/model/phrase-table.gz").ifexists
  rm(WORKING + "/model/phrase-table-sorted.gz").ifexists
  rm(WORKING + "/model/phrase-table-sorted").ifexists
  rm(WORKING + "/model/reordering-table.sorted").ifexists
  rm(WORKING + "/model/reordering-table").ifexists
  rm(WORKING + "/imodel/aligned-grow-diag-final-and").ifexists
  rm(WORKING + "/imodel/extract.inv.sorted.gz").ifexists
  rm(WORKING + "/imodel/extract.o.sorted.gz").ifexists
  rm(WORKING + "/imodel/extract.sorted.gz").ifexists
  rm(WORKING + "/imodel/lex.e2f").ifexists
  rm(WORKING + "/imodel/lex.e2f.tmp").ifexists
  rm(WORKING + "/imodel/lex.f2e").ifexists
  rm(WORKING + "/imodel/lex.f2e.tmp").ifexists
  rm(WORKING + "/imodel/phrase-table-all").ifexists
  rm(WORKING + "/imodel/phrase-table.gz").ifexists
  rm(WORKING + "/imodel/phrase-table-sorted.gz").ifexists
  rm(WORKING + "/imodel/phrase-table-sorted").ifexists
  rm(WORKING + "/imodel/reordering-table.sorted").ifexists
  rm(WORKING + "/imodel/reordering-table").ifexists
}

if(mert) {
  Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l1) < ("corpus/dev-%s-%s.%s" % (l1,l2,l1)) > (WORKING + "/dev-%s-%s.tok.%s" % (l1,l2,l1))

  Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l1) < (WORKING + "/dev-%s-%s.tok.%s" %
    (l1,l2,l1)) > (WORKING + "/dev-%s-%s.true.%s" % (l1,l2,l1))

  Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l2) < ("corpus/dev-%s-%s.%s" % (l1,l2,l2)) > (WORKING + "/dev-%s-%s.tok.%s" % (l1,l2,l2))

  Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l2) < (WORKING + "/dev-%s-%s.tok.%s" %
    (l1,l2,l2)) > (WORKING + "/dev-%s-%s.true.%s" % (l1,l2,l2))

  subTask("scripts/write-mosesini.scala",
    WORKING + "/model/moses.ini",
    WORKING + "/model",
    WORKING + "/lm/" + l1,"-forMert")

  Do(MOSES_DIR+"/mosesdecoder/scripts/training/mert-moses.pl",
    WORKING + "/dev-%s-%s.true.%s" % (l1,l2,l1),
    WORKING + "/dev-%s-%s.true.%s" % (l1,l2,l2),
    MOSES_DIR+"/mosesdecoder/bin/moses",
    WORKING + "/model/moses.ini",
    "--pairwise-ranked",
    "--mertdir",MOSES_DIR+"/mosesdecoder/bin",
    "--decoder-flags=-threads "+heads+" -s 10") > (WORKING + "/model/mert.out") err (WORKING + "/model/mert.err") dir (WORKING + "/model")
}

subTask("scripts/write-mosesini.scala",
  WORKING + "/model/moses.ini",
  WORKING + "/model",
  WORKING + "/lm/" + l1 + ".bin")

if(mert) {
  subTask("scripts/write-mosesini.scala",
    WORKING + "/imodel/moses.ini",
    WORKING + "/imodel",
    WORKING + "/lm/" + l2,"-forMert")

  Do(MOSES_DIR+"/mosesdecoder/scripts/training/mert-moses.pl",
    WORKING + "/dev-%s-%s.true.%s" % (l1,l2,l2),
    WORKING + "/dev-%s-%s.true.%s" % (l1,l2,l1),
    MOSES_DIR+"/mosesdecoder/bin/moses",
    WORKING + "/imodel/moses.ini",
    "--pairwise-ranked",
    "--mertdir",MOSES_DIR+"/mosesdecoder/bin",
    "--decoder-flags=-threads "+heads+" -s 10") > (WORKING + "/imodel/mert.out") err (WORKING + "/imodel/mert.err") dir (WORKING + "/imodel")
}

subTask("scripts/write-mosesini.scala",
  WORKING + "/imodel/moses.ini",
  WORKING + "/imodel",
  WORKING + "/lm/" + l2 +".bin")

Do(MOSES_DIR+"/mosesdecoder/bin/processPhraseTable","-ttable","0","0",
  WORKING + "/model/phrase-table-filtered.gz",
  "-nscores","5","-out",
  WORKING + "/model/phrase-table.bin")

Do(MOSES_DIR+"/mosesdecoder/bin/processPhraseTable","-ttable","0","0",
  WORKING + "/imodel/phrase-table-filtered.gz",
  "-nscores","5","-out",
  WORKING + "/imodel/phrase-table.bin")

Do(MOSES_DIR+"/mosesdecoder/bin/processLexicalTable",
  "-in",WORKING + "/model/reordering-table.wbe-msd-bidirectional-fe.gz",
  "-out",WORKING + "/model/reordering-table")

Do(MOSES_DIR+"/mosesdecoder/bin/processLexicalTable",
  "-in",WORKING + "/imodel/reordering-table.wbe-msd-bidirectional-fe.gz",
  "-out",WORKING + "/imodel/reordering-table")

def updateMosesIni(fileName : String) {
  val in = io.Source.fromFile(fileName)
  val lines = in.getLines.toList
  in.close
  val out = new java.io.PrintWriter(fileName)
  for(line <- lines) {
    if(line endsWith "phrase-table") {
      out.println(line + ".bin")
    } else if(line endsWith "phrase-table-filtered.gz") {
      System.err.println("This one!")
      out.println(line.replace("phrase-table-filtered.gz","phrase-table.bin"))
    } else {
      out.println(line)
    }
  }
  out.flush
  out.close
}

namedTask("update model/moses.ini") {
  updateMosesIni(WORKING + "/model/moses.ini")
}

namedTask("update imodel/moses.ini") {
  updateMosesIni(WORKING + "/imodel/moses.ini")
}

if(clean && mert) {
  rm(WORKING + ("/dev-%s-%s.tok.%s" % (l1,l2,l1))).ifexists
  rm(WORKING + ("/dev-%s-%s.tok.%s" % (l1,l2,l2))).ifexists
  rm(WORKING + ("/dev-%s-%s.true.%s" % (l1,l2,l1))).ifexists
  rm(WORKING + ("/dev-%s-%s.true.%s" % (l1,l2,l2))).ifexists
  rm(WORKING + "/model/mert.err").ifexists
  rm(WORKING + "/model/mert.out").ifexists
  rm(WORKING + "/model/mert-work").ifexists.r
  rm(WORKING + "/imodel/mert.err").ifexists
  rm(WORKING + "/imodel/mert.out").ifexists
  rm(WORKING + "/imodel/mert-work").ifexists.r
}
