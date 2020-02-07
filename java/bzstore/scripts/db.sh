#!/usr/bin/env bash

mkdir -p ~/logs

wdb_home="/home/ubuntu/bzs/java/bzstore/scripts"
git_update="git pull origin master"

clusterNumber=$1
clusterIPFile="c$clusterNumber"

clusterNodes=($(cat $clusterIPFile))
leaderIP=${clusterNodes[0]}

function run_command() {
  ip=$1
  command=$2
  ssh -i cluster0_0.pem $ip ". ~/.profile ; cd $wdb_home; $command &> db.log &" &
}
function push_file() {
  ip=$1
  file_name=$3
  src_file_name=$2
  scp -i cluster0_0.pem $src_file_name $ip:"$wdb_home/$file_name"
}

function get_file() {
  ip=$1
  file_name=$2
  dest_file_name=$3
  scp -i cluster0_0.pem $ip:"$wdb_home/$file_name" $dest_file_name
}

function run_command_on_all_nodes() {
  command=$1
  for i in $(cat $clusterIPFile); do
    echo "========"
    echo "==== For IP $i"
    echo "========"
    run_command $i "$command"
    #        ssh -i cluster0_0.pem $i  "cd $wdb_home; $command; cd -; " ;
  done
}

if [ -f "$clusterIPFile" ]; then
  echo "loading IPs from $clusterIPFile"
  cluster="$clusterIPFile"
else
  echo "Cannot proceed."
  echo "usage: db.sh clusterNumber [uprepo|stop|start|clean|build]"
  exit 1
fi

if [[ "$2" == "uprepo" ]]; then
  run_command_on_all_nodes "$git_update"
elif [[ "$2" == "stop" ]]; then
  run_command_on_all_nodes "./wdb.sh stop"
elif [[ "$2" == "starty" ]]; then
  echo "Starting wedgeDB cluster $clusterNumber, WITH BENCHMARKS***"
  echo "========="
  echo "== Cluster Node $clusterNumber $i"
  run_command $leaderIP "./bzs.sh $clusterNumber 0 y"

  for i in {1,2,3}; do
    echo "========="
    echo "== Cluster Node $clusterNumber $i"
    run_command ${clusterNodes[$i]} "./bzs.sh $clusterNumber $i"
    echo "========="
  done
elif [[ "$2" == "start" ]]; then
  echo "Starting wedgeDB cluster $clusterNumber"
  for i in {0,1,2,3}; do
    echo "========="
    echo "== Cluster Node $clusterNumber $i"
    run_command ${clusterNodes[$i]} "./bzs.sh $clusterNumber $i"
    echo "========="
  done
elif [[ "$2" == "clean" ]]; then
  run_command_on_all_nodes "./bzs-setup.sh cleanDB"
elif [[ "$2" == "build" ]]; then
  run_command_on_all_nodes "./bzs-setup.sh install"
elif [[ "$2" == "log" ]]; then
  nodeNumber=$3
  suffix=$(date +"%m-%d-%y_%H-%M-%S")
  dest_file_name="./db_$clusterNumber-$nodeNumber-$suffix.log"
  get_file ${clusterNodes[$nodeNumber]} "db.log" "$dest_file_name"
  less $dest_file_name
elif [[ "$2" == "bftclear" ]]; then
  run_command_on_all_nodes "rm ./config/currentView"
elif [[ "$2" == "copydb" ]]; then
  for i in $(cat $clusterIPFile); do
    push_file $i "$wdb_home/data.txt" "data.txt"
  done
elif [[ "$2" == "logall" ]]; then
  suffix=$(date +"%m-%d-%y_%H-%M-%S")
  let n=0
  mkdir -p "./logs"
  for i in $(cat $clusterIPFile); do
    dest_file_name=".logs/db_$clusterNumber-$n-$suffix.log"
    get_file $i "db.log" "$dest_file_name"
    let n++
  done
fi
