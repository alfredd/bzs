#!/usr/bin/env bash


java -Djava.security.properties="./config/java.security" -Dlogback.configurationFile="./config/logback.xml" -cp BFT-SMaRt.jar:bzstore-1.0-SNAPSHOT-jar-with-dependencies.jar:merkle-b-tree-1.0-SNAPSHOT.jar:bcpkix-jdk15on-160.jar:bcprov-jdk15on-160.jar:netty-all-4.1.34.Final.jar edu.ucsc.edgelab.db.bzs.clients.DBTracerClient "$1" "$2"