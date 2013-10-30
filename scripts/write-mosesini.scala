val outFile = opts.woFile("moses.ini","Target moses.ini file")
val prefix = opts.string("prefix","The prefix location")
val lmFile = opts.string("lmFile","The path of the language model file")
val forMert = opts.flag("forMert","Write a MERT compatible ini file")
opts.verify

namedTask("Write moses.ini") {
val out = opts.openOutput(outFile)

out.println("""#########################
### MOSES CONFIG FILE ###
#########################

# input factors
[input-factors]
0

# mapping steps
[mapping]
0 T 0

# translation tables: table type (hierarchical(0), textual (0), binary (1)), source-factors, target-factors, number of scores, file 
# OLD FORMAT is still handled for back-compatibility
# OLD FORMAT translation tables: source-factors, target-factors, number of scores, file 
# OLD FORMAT a binary table type (1) is assumed 
[ttable-file]
""" +(
  if(forMert) {
    "0 0 0 5 " + prefix + "/phrase-table-filtered.gz"
  } else {
    "1 0 0 5 " + prefix + "/phrase-table"
  }) + """
# no generation models, no generation-file section

# language models: type(srilm/irstlm), factors, order, file
[lmodel-file]
8 0 3 """ + lmFile + """

# limit on how many phrase translations e for each phrase f are loaded
# 0 = all elements loaded
[ttable-limit]
20

# distortion (reordering) files
[distortion-file]
""" + (
  if(forMert) {
    "0-0 wbe-msd-bidirectional-fe-allff 6 " + prefix + "/reordering-table.wbe-msd-bidirectional-fe.gz"
  } else {
    "0-0 wbe-msd-bidirectional-fe-allff 6 " + prefix + "/reordering-table"
  }))

if(forMert || !(new java.io.File(prefix + "/mert-work/moses.ini").exists())) {
  out.println("""# distortion (reordering) weight
[weight-d]
0.3
0.3
0.3
0.3
0.3
0.3
0.3

# language model weights
[weight-l]
0.5000


# translation model weights
[weight-t]
0.20
0.20
0.20
0.20
0.20

# no generation models, no weight-generation section

# word penalty
[weight-w]
-1

[distortion-limit]
6""")
} else {
  val in = io.Source.fromFile(prefix + "/mert-work/moses.ini").getLines
  val in2 = in.dropWhile(_ != "[weight-d]")
  for(line <- in2) {
    out.println(line)
  }
}
out.flush
out.close
}
