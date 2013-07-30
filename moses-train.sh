#!/bin/bash

# Check setup
die () {
    echo >&2 "$@"
    exit 1
}

if [ $# -ne 3 ] && [ $# -ne 2 ]
then
    die "Usage: bash moses-train.sh en es [linesPerSplit]"
fi

LM=$2
if [[ $1 < $2 ]]
then
    L1=$1
    L2=$2
else
    L1=$2
    L2=$1
fi
WORKING=`pwd`/working/$L2-$L1
SPLIT_SIZE=$3
HEADS=4
export IRSTLM=`pwd`/irstlm

mkdir -p $WORKING/model

if [ ! -e $WORKING/corpus-$L1-$L2.clean.$1 ]
then
    cd corpus
    echo "Prepare corpus $L1"
    gunzip corpus-$L1-$L2.$L1.gz
    ../mosesdecoder/scripts/tokenizer/tokenizer.perl -l $L1 < corpus-$L1-$L2.$L1 > corpus-$L1-$L2.tok.$L1 || die "$L1 Tokenization"
    ../mosesdecoder/scripts/recaser/truecase.perl --model ../truecaser/truecase.$L1 < corpus-$L1-$L2.tok.$L1 > corpus-$L1-$L2.true.$L1 || die "$L1 Truecasing"
    rm corpus-$L1-$L2.tok.$L1
    gzip corpus-$L1-$L2.$L1

    echo "Prepare corpus $L2"
    gunzip corpus-$L1-$L2.$L2.gz
    ../mosesdecoder/scripts/tokenizer/tokenizer.perl -l $L2 < corpus-$L1-$L2.$L2 > corpus-$L1-$L2.tok.$L2 || die "$L2 Tokenization"
    ../mosesdecoder/scripts/recaser/truecase.perl --model ../truecaser/truecase.$L2 < corpus-$L1-$L2.tok.$L2 > corpus-$L1-$L2.true.$L2 || die "$L1 Truecasing"
    rm corpus-$L1-$L2.tok.$L2
    gzip corpus-$L1-$L2.$L2

    cd ..

    echo "Begin clean"
    ~/moses/mosesdecoder/scripts/training/clean-corpus-n.perl corpus/corpus-$L1-$L2.true $L1 $L2 $WORKING/corpus-$L1-$L2.clean 1 80 || die "Cleaning"
    rm corpus/corpus-$L1-$L2.true.*
fi

# Build language model
if [[ ! -e $WORKING/../lm/$L1 ]]
then
    echo "Build language model for $L1"
    mkdir -p $WORKING/../lm
    rm $WORKING/../lm/$L1*
    irstlm/bin/add-start-end.sh < $WORKING/corpus-$L1-$L2.clean.$L1 > $WORKING/corpus-$L1-$L2.sb.$L1 || die "Add Start End"
    irstlm/bin/build-lm.sh -i $WORKING/corpus-$L1-$L2.sb.$L1 -t tmp -p -s improved-kneser-ney -o $WORKING/../lm/$L1 || die "Build LM"
    irstlm/bin/compile-lm --text yes $WORKING/../lm/$L1.gz $WORKING/../lm/$L1 || die "Compile LM"
fi

# Build language model
if [[ ! -e $WORKING/../lm/$L2 ]]
then
    echo "Build language model for $L2"
    mkdir -p $WORKING/../lm
    rm $WORKING/../lm/$L2*
    irstlm/bin/add-start-end.sh < $WORKING/corpus-$L1-$L2.clean.$L2 > $WORKING/corpus-$L1-$L2.sb.$L2 || die "Add Start End"
    irstlm/bin/build-lm.sh -i $WORKING/corpus-$L1-$L2.sb.$L2 -t tmp -p -s improved-kneser-ney -o $WORKING/../lm/$L2 || die "Build LM"
    irstlm/bin/compile-lm --text yes $WORKING/../lm/$L2.gz $WORKING/../lm/$L2 || die "Compile LM"
fi

if [[ -z $SPLIT_SIZE ]]
then
    echo "Calculate word alignment"
    ionice -c 3 nice ../cdec/corpus/paste-files.pl $WORKING/corpus-$L1-$L2.clean.$L1 $WORKING/corpus-$L1-$L2.clean.$L2 > $WORKING/corpus-train.$L1-$L2
    ionice -c 3 nice ../cdec/word-aligner/fast_align -i $WORKING/corpus-train.$L1-$L2 -d -v -o > $WORKING/$L1-$L2.fwd_align
    ionice -c 3 nice ../cdec/word-aligner/fast_align -i $WORKING/corpus-train.$L1-$L2 -d -v -o -r > $WORKING/$L1-$L2.rev_align
    ionice -c 3 nice ../cdec/utils/atools -i $WORKING/$L1-$L2.fwd_align -j $WORKING/$L1-$L2.rev_align -c grow-diag-final-and > $WORKING/model/aligned.grow-diag-final-and
    ionice -c 3 nice ../cdec/utils/atools -i $WORKING/$L1-$L2.rev_align -j $WORKING/$L1-$L2.fwd_align -c grow-diag-final-and > $WORKING/imodel/aligned.grow-diag-final-and

    echo "Build phrase table"
    # Create translation model
    LMFILE=`readlink -f $WORKING/../lm/$L1`
    ionice -c 3 nice mosesdecoder/scripts/training/train-model.perl -do-steps 4-9 -root-dir $WORKING -corpus $WORKING/corpus-$L1-$L2.clean -f $L2 -e $L1 -alignment grow-diag-final-and -reordering msd-bidirectional-fe -lm 0:3:$LMFILE:8 -external-bin-dir tools || die "Moses train failed"
    LMFILE=`readlink -f $WORKING/../lm/$L2`
    ionice -c 3 nice mosesdecoder/scripts/training/train-model.perl -do-steps 4-9 -root-dir $WORKING -corpus $WORKING/corpus-$L1-$L2.clean -model-dir $WORKING/imodel -f $L1 -e $L2 -alignment grow-diag-final-and -reordering msd-bidirectional-fe -lm 0:3:$LMFILE:8 -external-bin-dir tools || die "Moses train failed"
else
    echo "Splitting $L1 file"
    split -l $SPLIT_SIZE $WORKING/corpus-$L1-$L2.clean.$L1 $WORKING/corpus.$L1.
    echo "Splitting $L2 file"
    split -l $SPLIT_SIZE $WORKING/corpus-$L1-$L2.clean.$L2 $WORKING/corpus.$L2.

    echo "Starting heads"
    I=0
    for L1FILE in `ls $WORKING/corpus.$L1.*`
    do
        if [ -d $WORKING/$I ]
        then
            rm -fr $WORKING/$I
        fi
        mkdir $WORKING/$I
        L2FILE=`echo $L1FILE | sed -r "s/$L1\.(.*)/$L2\.\1/"`
        mv $L1FILE $WORKING/$I/corpus.$L1
        mv $L2FILE $WORKING/$I/corpus.$L2
        HEAD=$[$I % $HEADS]
        PARTS[$HEAD]="${PARTS[$HEAD]} $WORKING/$I"
        I=$[$I + 1]
    done

    for (( i=0; i<$[$HEADS - 1]; i++ ))
    do
        screen -S moses$i -d -m sh -c "bash moses-train-head.sh $WORKING $L1 $L2 $LM ${PARTS[$i]} 2>&1 | tee out$i"
    done
    i=$[$HEADS - 1]
    screen -S moses$i sh -c "bash moses-train-head.sh $WORKING $L1 $L2 $LM ${PARTS[$i]} 2>&1 | tee out$i"

    echo "Merge Lex e2f"
    scala merge-lex.scala `find $WORKING -name lex.e2f` > $WORKING/lex.e2f
    echo "Merge Lex f2e"
    scala merge-lex.scala `find $WORKING -name lex.f2e` > $WORKING/lex.f2e
    echo "Merge phrase table"
    zcat `find $WORKING -name phrase-table.filtered.gz` | LC_ALL=C sort | JAVA_OPTS=-Xmx2g scala -cp MapDB-0.9-SNAPSHOT.jar merge-pts.scala $WORKING/lex.e2f $WORKING/lex.f2e | gzip > $WORKING/phrase-table.gz
fi

# Test translation
#echo "eine Übersetzung" | mosesdecoder/bin/moses -f working/train/model/moses.ini

