#!/bin/bash

if [[ $1 == "-cp" ]]
then 
    CLASSPATH=target/Nimrod-assembly-0.13.7.jar:lib/\*:$2
    shift
    shift
else
    CLASSPATH=target/Nimrod-assembly-0.13.7.jar:lib/\*
fi

exec java -cp $CLASSPATH nimrod.Main "$@"
