val corpus = opts.roFile("corpus","The corpus to read")
val lm = opts.string("language","The language to use")
val out = opts.string("out","The name for the out files")
opts.restAsSystemProperties
opts.verify

val WORKING = System.getProperty("working",System.getProperty("user.dir") + "/working/" + out +"/")
val MOSES_DIR = System.getProperty("mosesDir",System.getProperty("user.home")+"/moses")

mkdir(WORKING).p

val (corpusName,corpusPath) = if(corpus.getName().endsWith(".gz")) {
  gunzip(corpus)
  (corpus.getName().replaceAll(".gz$",""),corpus.getPath().replaceAll(".gz$",""))
} else {
  (corpus.getName(),corpus.getPath())
}

val WORKING_CORPUS = WORKING + corpusName

Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",lm) < (corpusPath) > (WORKING_CORPUS + ".tok")

checkExists(MOSES_DIR+"/truecaser/truecase."+lm)

Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+lm) < (WORKING_CORPUS + ".tok") >
(WORKING_CORPUS + ".true")

Do(MOSES_DIR+"/irstlm/bin/add-start-end.sh") < (WORKING_CORPUS + ".true") > (WORKING_CORPUS + ".sb")

rm(WORKING + out + "." + lm + ".tmp").ifexists

Do(MOSES_DIR+"/irstlm/bin/build-lm.sh",
  "-i",WORKING_CORPUS + ".sb",
  "-t","tmp",
  "-p","-s","improved-kneser-ney",
  "-o",WORKING + out+"." + lm + ".tmp").env("IRSTLM",MOSES_DIR+"/irstlm")

Do (MOSES_DIR+"/irstlm/bin/compile-lm",
  "--text","yes",
  WORKING + out+"." + lm + ".tmp.gz",
  WORKING + out+"." + lm + ".tmp")

subTask("scripts/remove-zeros.scala",
  WORKING + out+"." + lm + ".tmp",
  WORKING + out+"." + lm )

Do(MOSES_DIR+"/mosesdecoder/bin/build_binary",
  WORKING + out+"." + lm ,
  WORKING + out+"." + lm + ".bin")
