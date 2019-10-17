#!/usr/bin/env bash

java -version
if [[ "$?" != "0" ]]
then
    echo "Cannot run BZS replica. java is not installed. Install java to continue"
    exit 1
fi

if [[ "$#" -lt "2" ]]
then
    echo "Usage: $0 CLUSTER_ID REPLICA_ID"
    echo "      where ID=0|1|2|3"
    exit 1
fi

java -Djava.security.properties="./config/java.security" -Dlogback.configurationFile="./config/logback.xml" -cp BFT-SMaRt.jar:bzstore-1.0-SNAPSHOT-jar-with-dependencies.jar:merkle-b-tree-1.0-SNAPSHOT.jar:bcpkix-jdk15on-160.jar:bcprov-jdk15on-160.jar:netty-all-4.1.34.Final.jar edu.ucsc.edgelab.db.bzs.replica.BZStoreServer "$1" "$2"