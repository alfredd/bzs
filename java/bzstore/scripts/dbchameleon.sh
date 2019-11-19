#!/usr/bin/env bash

mkdir -p ~/logs

wdb_home="/home/cc/bzs/java/bzstore/scripts"
git_update="git pull origin master"

clusterNumber=$1
clusterIPFile="c$clusterNumber"

numNodes=4
clusterNodes=(`cat $clusterIPFile`)
leaderIP=${clusterNodes[0]}

function run_command {
    ip=$1
    command=$2
    if [ -z "$3" ]
    then 
      ssh -i chameleonkey.pem cc@$ip  ". ~/.profile ; cd $wdb_home; $command &> db.log &" &
    else
      #running command at home
      ssh -i chameleonkey.pem cc@$ip  ". ~/.profile ; $command"  
    fi
}

#update replica ips for node-to-node communication
function update_replica_ips {
  seqEnd=$((numNodes-1))
  for i in $(seq 0 $seqEnd)
  do
    replica=r$i
    replicaIp="$(openstack server show $replica | grep sharednet | cut -d '=' -f 2 | cut -d ',' -f 1)"
    echo "initial ip for $replica is ${replica_ips[$i]}"
    replica_ips[$i]=$replicaIp
    echo "updated ip for $replica is ${replica_ips[$i]}"
  done
  #openstack server show c0 | grep sharednet | cut -d '=' -f 2 | cut -d ',' -f 1
}



#update the hosts.config file with latest ips 
function update_config {
  if [ -f "hosts.config" ]
  then
    rm hosts.config 
  fi  

  cp bftsmart_conf/hosts.config.stub hosts.config 
	
	rcport=11000
  rrport=11001
  for i in "${!replica_ips[@]}"; do
    echo "$i ${replica_ips[$i]} $rcport $rrport" >> hosts.config 
		rcport=$(( rcport + 10 )) 
    rrport=$(( rrport + 10 )) 
  done
  
  echo "" >> hosts.config 
  echo $ttp_ip >> hosts.config 
}



function get_file {
    ip=$1
    file_name=$2
    dest_file_name=$3
    scp -i chameleonkey.pem cc@$ip:"$wdb_home/$file_name" $dest_file_name
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

source ./nodes.sh 

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
elif [[ "$2" == "setup" ]]
then
    if [[ "$3" == "copy" ]]
    then
      echo "copying files onto machines"
      for i in  `cat $clusterIPFile` ;
      do
        echo "copying github key"
        scp github_rsa cc@$i:"~/.ssh/"   
        echo "copying init.sh"
        scp init.sh cc@$i:"~/"  
        echo "copying jdk"
        scp ./jdk-10.0.2_linux-x64_bin.tar.gz cc@$i:~/ & 
      done 
    elif [[ "$3" == "init" ]]
    then
      echo "running init.sh"
      run_command_on_all_nodes "./init.sh"
    elif [[ "$3" == "config" ]]
    then
      echo "configuring node ips"
      update_replica_ips
      update_config
      j=0
      for i in  `cat $clusterIPFile` ;
      do
      	scp hosts.config cc@$i:"$wdb_home/config/"
      	sed "s/= auto/= ${replica_ips[$j]}/" bftsmart_conf/system.config.bak > system.config
      	scp system.config cc@${replica_floating_ips[$j]}:"$wdb_home/config/"  
        j=$(( j + 1 ))
      done
    fi


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
    echo "building"
    run_command_on_all_nodes "./bzs-setup.sh install"
elif [[ "$2" == "log" ]]
then
    nodeNumber=$3
    dest_file_name="./db_$clusterNumber-$nodeNumber.log"
    get_file ${clusterNodes[$nodeNumber]} "db.log" "$dest_file_name"
    less $dest_file_name
elif [[ "$2" == "bftclear" ]]
then
    run_command_on_all_nodes "rm ./config/currentView"
fi
