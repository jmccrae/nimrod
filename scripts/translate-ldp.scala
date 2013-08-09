val ldpFile = opts.roFile("ldpFile","The lemon design patterns file")
val srcLang = opts.string("srcLang","The source language string")
val trgLang = opts.string("trgLang","The target langauge string")
opts.verify

val trgDir = ldpFile.getParentFile().getParent() + "/" + trgLang

val toTranslateFile = new File(ldpFile.getPath() + ".tt")
val translations = new File(ldpFile.getPath() + ".trans")
val out = new File(trgDir + "/"+ldpFile.getName())

mkdir(trgDir).p

subTask("scripts/rdf-extract.scala",ldpFile.path,toTranslateFile.path)

mj("eu.monnetproject.translation.controller.RTPL",List(srcLang,trgLang),pom="../monnet/translation/controller/pom.xml") < toTranslateFile > 
  translations

subTask("scripts/ldp-merge.scala",toTranslateFile.getCanonicalPath(),
  translations.getCanonicalPath(),ldpFile.path,out.path)
