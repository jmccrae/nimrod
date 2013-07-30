val opts = new Opts(args)
val l1tmp = opts.string("srcLang","The source language")
val l2tmp = opts.string("trgLang","The target langauge")
opts.verify

val l1 = List(l1tmp,l2tmp).min
val l2 = List(l1tmp,l2tmp).max
val WORKING = System.getProperty("user.dir") + "/working/" + l1 + "-" + l2
val heads = 4
val MOSES_DIR= System.getProperty("mosesDir","/home/jmccrae/moses")

//export IRSTLM=`pwd`/irstlm

mkdir(WORKING + "/model").p

block("Prepare corpus " + l1) {
  gunzip("corpus/corpus-%s-%s.%s.gz" % (l1,l2,l1))
  Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l1) < ("corpus/corpus-%s-%s.%s" % (l1,l2,l1)) > ("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l1))
  checkExists(MOSES_DIR+"/truecaser/truecase."+l1)
  Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l1) < ("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l1)) > ("corpus/corpus-%s-%s.true.%s" % (l1,l2,l1))
  rm("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l1))
}

block("Prepare corpus " + l2) {
  gunzip("corpus/corpus-%s-%s.%s.gz" % (l1,l2,l2))
  Do(MOSES_DIR+"/mosesdecoder/scripts/tokenizer/tokenizer.perl","-l",l2) < ("corpus/corpus-%s-%s.%s" % (l1,l2,l2)) > ("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l2))
  checkExists(MOSES_DIR+"/truecaser/truecase."+l2)
  Do(MOSES_DIR+"/mosesdecoder/scripts/recaser/truecase.perl","--model",MOSES_DIR+"/truecaser/truecase."+l2) < ("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l2)) > ("corpus/corpus-%s-%s.true.%s" % (l1,l2,l2))
  rm("corpus/corpus-%s-%s.tok.%s" % (l1,l2,l2))
}

block("Clean corpus ") {
  Do(MOSES_DIR+"/mosesdecoder/scripts/training/clean-corpus-n.perl",
    "corpus/corpus-%s-%s.true" % (l1,l2),
    l1,l2,WORKING + "/corpus-%s-%s.clean" % (l1,l2),"1","80")
  rm(("corpus/corpus-%s-%s.true.%s" % (l1,l2,l1)))
  rm(("corpus/corpus-%s-%s.true.%s" % (l1,l2,l2)))
}

//# Build language model
//if [[ ! -e $WORKING/../lm/$L1 ]]
//then
//    echo "Build language model for $L1"
//    mkdir -p $WORKING/../lm
//    rm $WORKING/../lm/$L1*
//    irstlm/bin/add-start-end.sh < $WORKING/corpus-$L1-$L2.clean.$L1 > $WORKING/corpus-$L1-$L2.sb.$L1 || die "Add Start End"
//    irstlm/bin/build-lm.sh -i $WORKING/corpus-$L1-$L2.sb.$L1 -t tmp -p -s improved-kneser-ney -o $WORKING/../lm/$L1 || die "Build LM"
//    irstlm/bin/compile-lm --text yes $WORKING/../lm/$L1.gz $WORKING/../lm/$L1 || die "Compile LM"
//fi
//
//# Build language model
//if [[ ! -e $WORKING/../lm/$L2 ]]
//then
//    echo "Build language model for $L2"
//    mkdir -p $WORKING/../lm
//    rm $WORKING/../lm/$L2*
//    irstlm/bin/add-start-end.sh < $WORKING/corpus-$L1-$L2.clean.$L2 > $WORKING/corpus-$L1-$L2.sb.$L2 || die "Add Start End"
//    irstlm/bin/build-lm.sh -i $WORKING/corpus-$L1-$L2.sb.$L2 -t tmp -p -s improved-kneser-ney -o $WORKING/../lm/$L2 || die "Build LM"
//    irstlm/bin/compile-lm --text yes $WORKING/../lm/$L2.gz $WORKING/../lm/$L2 || die "Compile LM"
//fi
//
//if [[ -z $SPLIT_SIZE ]]
//then
//    echo "Calculate word alignment"
//    ionice -c 3 nice ../cdec/corpus/paste-files.pl $WORKING/corpus-$L1-$L2.clean.$L1 $WORKING/corpus-$L1-$L2.clean.$L2 > $WORKING/corpus-train.$L1-$L2
//    ionice -c 3 nice ../cdec/word-aligner/fast_align -i $WORKING/corpus-train.$L1-$L2 -d -v -o > $WORKING/$L1-$L2.fwd_align
//    ionice -c 3 nice ../cdec/word-aligner/fast_align -i $WORKING/corpus-train.$L1-$L2 -d -v -o -r > $WORKING/$L1-$L2.rev_align
//    ionice -c 3 nice ../cdec/utils/atools -i $WORKING/$L1-$L2.fwd_align -j $WORKING/$L1-$L2.rev_align -c grow-diag-final-and > $WORKING/model/aligned.grow-diag-final-and
//    ionice -c 3 nice ../cdec/utils/atools -i $WORKING/$L1-$L2.rev_align -j $WORKING/$L1-$L2.fwd_align -c grow-diag-final-and > $WORKING/imodel/aligned.grow-diag-final-and
//
//    echo "Build phrase table"
//    # Create translation model
//    LMFILE=`readlink -f $WORKING/../lm/$L1`
//    ionice -c 3 nice mosesdecoder/scripts/training/train-model.perl -do-steps 4-9 -root-dir $WORKING -corpus $WORKING/corpus-$L1-$L2.clean -f $L2 -e $L1 -alignment grow-diag-final-and -reordering msd-bidirectional-fe -lm 0:3:$LMFILE:8 -external-bin-dir tools || die "Moses train failed"
//    LMFILE=`readlink -f $WORKING/../lm/$L2`
//    ionice -c 3 nice mosesdecoder/scripts/training/train-model.perl -do-steps 4-9 -root-dir $WORKING -corpus $WORKING/corpus-$L1-$L2.clean -model-dir $WORKING/imodel -f $L1 -e $L2 -alignment grow-diag-final-and -reordering msd-bidirectional-fe -lm 0:3:$LMFILE:8 -external-bin-dir tools || die "Moses train failed"
//else
//    echo "Splitting $L1 file"
//    split -l $SPLIT_SIZE $WORKING/corpus-$L1-$L2.clean.$L1 $WORKING/corpus.$L1.
//    echo "Splitting $L2 file"
//    split -l $SPLIT_SIZE $WORKING/corpus-$L1-$L2.clean.$L2 $WORKING/corpus.$L2.
//
//    echo "Starting heads"
//    I=0
//    for L1FILE in `ls $WORKING/corpus.$L1.*`
//    do
//        if [ -d $WORKING/$I ]
//        then
//            rm -fr $WORKING/$I
//        fi
//        mkdir $WORKING/$I
//        L2FILE=`echo $L1FILE | sed -r "s/$L1\.(.*)/$L2\.\1/"`
//        mv $L1FILE $WORKING/$I/corpus.$L1
//        mv $L2FILE $WORKING/$I/corpus.$L2
//        HEAD=$[$I % $HEADS]
//        PARTS[$HEAD]="${PARTS[$HEAD]} $WORKING/$I"
//        I=$[$I + 1]
//    done
//
//    for (( i=0; i<$[$HEADS - 1]; i++ ))
//    do
//        screen -S moses$i -d -m sh -c "bash moses-train-head.sh $WORKING $L1 $L2 $LM ${PARTS[$i]} 2>&1 | tee out$i"
//    done
//    i=$[$HEADS - 1]
//    screen -S moses$i sh -c "bash moses-train-head.sh $WORKING $L1 $L2 $LM ${PARTS[$i]} 2>&1 | tee out$i"
//
//    echo "Merge Lex e2f"
//    scala merge-lex.scala `find $WORKING -name lex.e2f` > $WORKING/lex.e2f
//    echo "Merge Lex f2e"
//    scala merge-lex.scala `find $WORKING -name lex.f2e` > $WORKING/lex.f2e
//    echo "Merge phrase table"
//    zcat `find $WORKING -name phrase-table.filtered.gz` | LC_ALL=C sort | JAVA_OPTS=-Xmx2g scala -cp MapDB-0.9-SNAPSHOT.jar merge-pts.scala $WORKING/lex.e2f $WORKING/lex.f2e | gzip > $WORKING/phrase-table.gz
//fi
//
//# Test translation
//#echo "eine Übersetzung" | mosesdecoder/bin/moses -f working/train/model/moses.ini
//
