#!/usr/bin/env bash


function setup_workspace() {
    echo "Setting up workspace."
    sleep 1

    cp -rf ../../../library/lib/*.jar .
    cp -f ../../../library/bin/BFT-SMaRt.jar .
    cp -f ../../merkle-btree/target/merkle-b-tree-1.0-SNAPSHOT.jar .
    # Un-comment the next line if mvn is present and you want to build bzstore
    cp -rf ../target/bzstore-1.0-SNAPSHOT-jar-with-dependencies.jar .
}

if [[ "$1" == "setup" ]]
then
    setup_workspace
elif [[ "$1" == "bftsmart" ]]
then
    cp -rf ../../../library/config .
elif [[ "$1" == "cleanAll" ]]
then
    echo "Cleaning up workspace."
    rm -rf *.jar
    rm -rf BZS_data_*
elif [[ "$1" == "cleanDB" ]]
then
    echo "Cleaning up workspace."
    rm -rf BZS_data_*
elif [[ "$1" == "install" ]]
then
    java -version
    if [[ "$?" != "0" ]]
    then
        echo "java is not installed. Install java to continue"
        exit 1
    fi
    mvn -version
    if [[ "$?" != "0" ]]
    then
        echo "Maven is not installed. Install mvn to continue"
        exit 1
    fi
    cd ../..
    echo "Building bzstore"
    mvn clean install -DskipTests
        if [[ "$?" != "0" ]]
    then
        echo "Maven build failed. Rebuild again after fixing errors."
        exit 1
    fi
    cd -
    setup_workspace
else
    echo "Usage: $0 setup|install|clean"
    exit 1
fi

