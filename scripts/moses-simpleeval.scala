val modelFile = opts.roFile("model","The model to use for the experiments")
val testFile = opts.roFile("testFile","The file to translate")
val refFile = opts.roFile("refFile","The (reference) translation of the file") 
val l1 = opts.string("srcLang","The language translating from")
val l2 = opts.string("trgLang","The langauge translating to")
val out = opts.woFile("out","The file to write the BLEU scores to")
val retain = opts.flag("retain","Retain the translation and normalized reference")
opts.restAsSystemProperties
opts.verify

val MOSES_DIR = System.getProperty("mosesDir",System.getProperty("user.home")+"/moses")

Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l1) < testFile.getPath() > (testFile.getPath() + ".tok")

checkExists(MOSES_DIR+"/truecaser/truecase."+l1)

Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l1) < (testFile.getPath() + ".tok") >
(testFile.getPath() + ".true")

Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l2) < refFile.getPath() > (refFile.getPath() + ".tok")

checkExists(MOSES_DIR+"/truecaser/truecase."+l2)

Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l2) < (refFile.getPath() + ".tok") >
(refFile.getPath() + ".true")

Do(MOSES_DIR+"/mosesdecoder/bin/moses","-f",modelFile.getPath()) < (testFile.getPath() + ".true") > (testFile.getPath() + ".trans")

Do(MOSES_DIR+"/mosesdecoder/scripts/generic/multi-bleu.perl","-lc",(refFile.getPath() + ".true")) < (testFile.getPath() + ".trans") > out

Do("scala","scripts/mt/word-error-rate.scala","-lc",(refFile.getPath() + ".true")) < (testFile.getPath() + ".trans") >> out

rm(testFile.getPath() + ".tok")
rm(testFile.getPath() + ".true")
if(!retain) {
  rm(testFile.getPath() + ".trans")
}
rm(refFile.getPath() + ".tok")
if(!retain) {
  rm(refFile.getPath() + ".true")
}
