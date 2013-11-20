#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $1 == "-cp" ]]
then 
    CLASSPATH=$DIR/target/Nimrod-assembly-0.13.7.jar:$DIR/lib/\*:$2
    shift
    shift
else
    CLASSPATH=$DIR/target/Nimrod-assembly-0.13.7.jar:$DIR/lib/\*
fi

exec java -cp $CLASSPATH nimrod.Main "$@"
