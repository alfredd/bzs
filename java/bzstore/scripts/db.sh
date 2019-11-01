#!/usr/bin/env bash

mkdir -p ~/logs

wdb_home="/home/ubuntu/bzs/java/bzstore/scripts"
git_update="git pull origin master"

clusterNumber=$1
clusterIPFile="c$clusterNumber"

clusterNodes=(`cat $clusterIPFile`)
leaderIP=${clusterNodes[0]}

function run_command {
    ip=$1
    command=$2
    ssh -i cluster0_0.pem $ip  "cd $wdb_home; $command &> \"db.log\" & ; cd -; " ;
}


function run_command_on_all_nodes {
    command=$1
    for i in  `cat $clusterIPFile` ;
    do
        echo "========"; echo "==== For IP $i"; echo "========";
        run_command $i "$command"
#        ssh -i cluster0_0.pem $i  "cd $wdb_home; $command; cd -; " ;
    done
}

if [ -f "$clusterIPFile" ]
then
    echo "loading IPs from $clusterIPFile"
    cluster="$clusterIPFile"
else
    echo "Cannot proceed."
    echo "usage: db.sh clusterNumber [uprepo|stop|start|clean|build]"
    exit 1
fi

if [[ "$2" == "uprepo" ]]
then
    run_command_on_all_nodes "$git_update"
elif [[ "$2" == "stop" ]]
then
    run_command_on_all_nodes "./wdb.sh stop"
elif [[ "$2" == "starty" ]]
then
    echo "Starting wedgeDB cluster $clusterNumber, WITH BENCHMARKS***"
    echo "========="
    echo "== Cluster Node $clusterNumber $i"
    run_command $leaderIP "./bzs.sh $clusterNumber 0 y"

    for i in {1,2,3}
    do
        echo "========="
        echo "== Cluster Node $clusterNumber $i"
        run_command ${clusterNodes[$i]} "./bzs.sh $clusterNumber $i"
        echo "========="
    done
elif [[ "$2" == "start" ]]
then
    echo "Starting wedgeDB cluster $clusterNumber"
    for i in {0,1,2,3}
    do
        echo "========="
        echo "== Cluster Node $clusterNumber $i"
        run_command ${clusterNodes[$i]} "./bzs.sh $clusterNumber $i"
        echo "========="
    done
elif [[ "$2" == "clean" ]]
then
    run_command_on_all_nodes "./bzs-setup.sh cleanDB"
elif [[ "$2" == "build" ]]
then
    run_command_on_all_nodes "./bzs-setup.sh install"
fi