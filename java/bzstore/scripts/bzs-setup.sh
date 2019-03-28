#!/usr/bin/env bash

if [[ "$1" == "setup" ]]
then

    cp -r ../../../library/config .
    cp ../../../library/bin/BFT-SMaRt.jar .
    cp ../target/bzstore-1.0-SNAPSHOT-jar-with-dependencies.jar .
elif [[ "$1" == "clean" ]]
then
    rm *.jar
    rm config -rf
fi

