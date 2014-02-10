#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $1 == "-cp" ]]
then 
    CLASSPATH=$DIR/executor/target/scala-2.10/nimrod-executor-assembly-0.14.1.jar:$2
    shift
    shift
else
    CLASSPATH=$DIR/executor/target/scala-2.10/nimrod-executor-assembly-0.14.1.jar
fi

exec java -cp $CLASSPATH nimrod.Main "$@"
