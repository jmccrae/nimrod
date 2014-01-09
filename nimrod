#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $1 == "-cp" ]]
then 
    CLASSPATH=$DIR/target/scala-2.10/Nimrod-assembly-0.14.1.jar:$DIR/lib/\*:$2
    shift
    shift
else
    CLASSPATH=$DIR/target/scala-2.10/Nimrod-assembly-0.14.1.jar:$DIR/lib/\*
fi

exec java -cp $CLASSPATH nimrod.Main "$@"
