#!/usr/bin/env bash

java -version
if [[ "$?" != "0" ]]
then
    echo "Cannot run BZS replica. java is not installed. Install java to continue"
    exit 1
fi

if [[ "$#" != "2" ]]
then
    echo "Usage: ./$0 ID"
    echo "      where ID=0|1|2|3"
    exit 1
fi

java -cp BFT-SMaRt.jar:bzstore-1.0-SNAPSHOT-jar-with-dependencies.jar edu.ucsc.edgelab.db.bzs.replica.BZStoreServer "$1"