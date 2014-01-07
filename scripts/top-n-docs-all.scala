opts.restAsSystemProperties()

for(i <- 16 to 25) {
  val nstr = i + "0k"
  subTask("scripts/top-n-docs.scala",
    "../oneta/training/europarl-v7.de-en.en",
    "../oneta/training/europarl-v7.de-en.de",
    "../oneta/working/europarl-ifrs.en-de.sim",
    (i*10000).toString,
    "working/pidc/europarl.de-en-"+nstr+".en","working/pidc/europarl.de-en-"+nstr+".de")

  Do("gzip","-c","working/pidc/europarl.de-en-"+nstr+".en") > "corpus/corpus-de-en.en.gz"
  Do("gzip","-c","working/pidc/europarl.de-en-"+nstr+".de") > "corpus/corpus-de-en.de.gz"

  subTask("scripts/moses-train.scala","en","de","working=working"+nstr)

  Do("moses","-f","working"+nstr+"/model/moses.ini") < "../oneta/data/ifrs-labels.de" > ("working"+nstr+"/ifrs.de-en")

  Do("../moses/mosesdecoder/scripts/generic/multi-bleu.perl","-lc","../oneta/data/ifrs-labels.en") < "working"+nstr+"/ifrs.de-en" >
    "working"+nstr+"/bleu"
}

