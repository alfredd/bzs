#!/usr/bin/env bash

mkdir -p ~/logs

#wdb_home="/home/cc/bzs/java/bzstore/scripts"
git_update="git pull origin master"

clusterNumber=$1
clusterIPFile="c$clusterNumber"

clusterNodes=($(cat $clusterIPFile))
leaderIP=${clusterNodes[0]}




function run_command() {
  ip=$1
  command=$2
  key=$3
  user=$4
  wdb_home=$5
  echo "running command for key $key and user $user at $wdb_home"
  ssh -i $key $user@$ip ". ~/.profile ; cd $wdb_home; $command &> db.log &" &
}
function push_file() {
  ip=$1
  file_name=$3
  src_file_name=$2
  key=$4 
  user=$5
  wdb_home=$6
  echo "running command for key $key and user $user at $wdb_home"
  scp -i $key $src_file_name $user@$ip:"$wdb_home/$file_name"
}

function get_file() {
  ip=$1
  file_name=$2
  dest_file_name=$3
  key=$4 
  user=$5
  wdb_home=$6
  echo "running command for key $key and user $user at $wdb_home"
  scp -i $key $user@$ip:"$wdb_home/$file_name" $dest_file_name
}

function run_command_on_all_nodes() {
  command=$1
  key=$2
  user=$3
  wdb_home=$4
  for i in $(cat $clusterIPFile); do
    echo "========"
    echo "==== For IP $i"
    echo "========"
    run_command $i "$command" $key $user $wdb_home 
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
  run_command_on_all_nodes "./wdb.sh stop" $3 $4 $5
elif [[ "$2" == "starty" ]]; then
  echo "Starting wedgeDB cluster $clusterNumber, WITH BENCHMARKS***"
  echo "========="
  echo "== Cluster Node $clusterNumber $i"
  run_command $leaderIP "./bzs.sh $clusterNumber 0 y" "aws_key" "ubuntu" $3

  for i in {1,2,3}; do
    echo "========="
    echo "== Cluster Node $clusterNumber $i"
    run_command ${clusterNodes[$i]} "./bzs.sh $clusterNumber $i" "aws_key" "ubuntu"
    echo "========="
  done
elif [[ "$2" == "start" ]]; then
  echo "Starting wedgeDB cluster $clusterNumber"
  for i in {0,1,2,3}; do
    echo "========="
    echo "== Cluster Node $clusterNumber $i"
    run_command ${clusterNodes[$i]} "./bzs.sh $clusterNumber $i" $3 $4 $5
    echo "========="
  done
elif [[ "$2" == "clean" ]]; then
  run_command_on_all_nodes "./bzs-setup.sh cleanDB" $3 $4 $5
elif [[ "$2" == "build" ]]; then
  run_command_on_all_nodes "./bzs-setup.sh install"
elif [[ "$2" == "log" ]]; then
  nodeNumber=$3
  suffix=$(date +"%m-%d-%y_%H-%M-%S")
  dest_file_name="./db_$clusterNumber-$nodeNumber-$suffix.log"
  get_file ${clusterNodes[$nodeNumber]} "db.log" "$dest_file_name" $3 $4
  less $dest_file_name
elif [[ "$2" == "bftclear" ]]; then
  run_command_on_all_nodes "rm ./config/currentView" $3 $4 $5
elif [[ "$2" == "copydb" ]]; then
  for i in $(cat $clusterIPFile); do
    #push_file $i "data.txt" "data.txt"
    #push_file $i "config.properties" "config.properties" $3 $4
    push_file $i "test.txt" "test.txt" $3 $4 $5
  done
elif [[ "$2" == "logall" ]]; then
  suffix=$(date +"%m-%d-%y_%H-%M-%S")
  let n=0
  mkdir -p "./logs"
  for i in $(cat $clusterIPFile); do
    dest_file_name="db_$clusterNumber-$n-$suffix.log"
    get_file $i "db.log" "$dest_file_name" $3 $4 $5
    mv "$dest_file_name" ./logs/
    let n++
    echo $i
  done
fi
