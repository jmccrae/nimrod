#!/bin/bash

l1=$1
l2=$2

if [ -z $l1 ]
then
    echo "Please specify languages"
    exit
fi

if [ -z $l2 ]
then
    echo "Please specify languages"
    exit
fi

mkdir -p corpus/$l1-$l2

cd corpus/$l1-$l2

if [ ! -e Europarl.$l1-$l2.$l1 ]
then
    wget http://opus.lingfil.uu.se/download.php?f=Europarl%2F$l1-$l2.txt.zip -O Europarl.$l1-$l2.txt.zip
    unzip Europarl.$l1-$l2.txt.zip
    cp Europarl.$l1-$l2.$l1 ../corpus-$l1-$l2.$l1
    cp Europarl.$l1-$l2.$l2 ../corpus-$l1-$l2.$l2
fi

if [ ! -e ECB.$l1-$l2.$l1 ]
then
    wget http://opus.lingfil.uu.se/download.php?f=ECB%2F$l1-$l2.txt.zip -O ECB.$l1-$l2.txt.zip
    unzip ECB.$l1-$l2.txt.zip
    cat ECB.$l1-$l2.$l1 >> ../corpus-$l1-$l2.$l1
    cat ECB.$l1-$l2.$l2 >> ../corpus-$l1-$l2.$l2
fi

cd ..

if [ ! -e getAlignmentWithText.pl ] 
then
    wget http://wt.jrc.it/lt/Acquis/JRC-Acquis.3.0/alignments/getAlignmentWithText.pl || die "Cannot download Perl extraction script"
fi

if [ ! -d $l1-$l2/$l1/ ]
then
    wget http://wt.jrc.it/lt/Acquis/JRC-Acquis.3.0/corpus/jrc-$l1.tgz || die "Cannot download corpus for $l2"
    tar xzvf jrc-$l1.tgz -C $l1-$l2/
fi

if [ ! -d $l1-$l2/$l2/ ]
then 
    wget http://wt.jrc.it/lt/Acquis/JRC-Acquis.3.0/corpus/jrc-$l2.tgz || die "Cannot download corpus for $l2"
    tar xzvf jrc-$l2.tgz -C $l1-$l2/
fi

cd $l1-$l2

if [ ! -e jrc-$l1-$l2.xml ]
then 
    wget http://wt.jrc.it/lt/Acquis/JRC-Acquis.3.0/alignments/jrc-$l1-$l2.xml.gz || die "Cannot download alignment"
    gunzip jrc-$l1-$l2.xml.gz
fi

if [ ! -e $l1-$l2/aligned.xml.gz ]
then
    cp ../getAlignmentWithText.pl .
    perl getAlignmentWithText.pl jrc-$l1-$l2.xml | gzip > aligned.xml.gz
fi

if [ ! -e acquis.$l1-$l2.$l1 ]
then
    zcat aligned.xml.gz | grep "<s1>" | perl -pi -e 's/<.?s1>//g' > acquis.$l1-$l2.$l1
    zcat aligned.xml.gz | grep "<s2>" | perl -pi -e 's/<.?s2>//g' > acquis.$l1-$l2.$l2
    cat acquis.$l1-$l2.$l1 >> ../corpus-$l1-$l2.$l1
    cat acquis.$l1-$l2.$l2 >> ../corpus-$l1-$l2.$l2
fi
