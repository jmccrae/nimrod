#!/bin/sh

if [[ $1 == "-cp" ]]
then 
    CLASSPATH=target/scala-2.9.2/Nimrod-assembly-0.13.7.jar:$2
    shift
    shift
else
    CLASSPATH=target/scala-2.9.2/Nimrod-assembly-0.13.7.jar
fi

exec java -cp $CLASSPATH nimrod.Main "$@"
