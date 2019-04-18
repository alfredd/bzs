#!/usr/bin/env bash

java -cp BFT-SMaRt.jar:bzstore-1.0-SNAPSHOT-jar-with-dependencies.jar edu.ucsc.edgelab.db.bzs.replica.RSAKeyPairGenerator $1
