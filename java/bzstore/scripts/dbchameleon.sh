#!/usr/bin/env bash

mkdir -p ~/logs

wdb_home="/home/cc/bzs/java/bzstore/scripts"
git_update="git pull origin master"

clusterNumber=$1
clusterIPFile="c$clusterNumber"
privateIPFile="cp$clusterNumber"

numNodes=4
clusterNodes=(`cat $clusterIPFile`)
leaderIP=${clusterNodes[0]}

key="mykey"
pem="$key"
#pem="$key.pem"

nodeReservation=c028c769-e9d8-4103-a0df-f7fb8581550f




function run_command {
    ip=$1
    command=$2
    if [ -z $3 ]
    then
      ssh -i $pem cc@$ip  ". ~/.profile ; cd $wdb_home; $command &> db.log &" &
    else
      #running command at home
			echo "running command at home"
      ssh -i $pem cc@$ip  ". ~/.profile ; $command"  
    fi
}

      #--image CC-Ubuntu16.04 \

#launch ubuntu instances on chameleon
function create_instances {
  seqEnd=$((numNodes-1))
  for i in $(seq 0 $seqEnd)
  do  
    nodeName="r$clusterNumber-$i"
    echo $nodeName 
    openstack server create \
      --image TransedgeNew \
      --flavor baremetal \
      --key-name $key \
      --nic net-id=sharednet1 \
      --hint reservation=$nodeReservation \
      --user-data instance_init.sh \
      $nodeName 
    sleep 10 #sleeps for a few seconds between calls
  done 
}



#update replica ips for node-to-node communication
function update_replica_ips {
  seqEnd=$((numNodes-1))
  for i in $(seq 0 $seqEnd)
  do
    replica=r$clusterNumber-$i
    replicaIp="$(openstack server show $replica | grep sharednet | cut -d '=' -f 2 | cut -d ',' -f 1)"
    echo "initial ip for $replica is ${replica_ips[$i]}"
    replica_ips[$i]=$replicaIp
    echo "updated ip for $replica is ${replica_ips[$i]}"
  done
  #openstack server show c0 | grep sharednet | cut -d '=' -f 2 | cut -d ',' -f 1
}


function update_replica_ips_from_file {
  readarray -t replica_ips < $privateIPFile
  echo "replica ips"
 for i in "${replica_ips[@]}"; do
    echo $i
 done  
}


#clear the floating ips
function clear_floating_ips {
	for i in  `cat $clusterIPFile`; do 
		ssh-keygen -f "/home/kohdmonkey/.ssh/known_hosts" -R $i
	done
}


function add_floating_ips {
  j=0
	for i in `cat $clusterIPFile`; do 
    replica=r$clusterNumber-$j
    echo "adding ip for replica $replica"
    openstack server add floating ip $replica $i
    j=$(( j + 1 ))
    sleep 5
  done
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
  j=0
  for i in "${replica_ips[@]}"; do
    echo "$j $i $rcport $rrport" >> hosts.config 
		rcport=$(( rcport + 10 )) 
    rrport=$(( rrport + 10 )) 
    j=$(( j + 1 ))
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
elif [[ "$2" == "new" ]]
then
    if [[ "$3" == "create" ]]
    then
			echo "creating new nodes"
			create_instances	
    elif [[ "$3" == "add_ip" ]]
    then
			echo "adding floating ips for replicas" 
			clear_floating_ips 
			add_floating_ips
		fi 
elif [[ "$2" == "setup" ]]
then
    if [[ "$3" == "copy" ]]
    then
      #clear_floating_ips
      echo "copying files onto machines"
      for i in  `cat $clusterIPFile` ;
      do
        #initial copy for initialization
        #scp c1 cc@$i:"/home/cc/bzs/java/bzstore/scripts/"
        : '
        echo "copying github key"
        scp github_rsa cc@$i:"~/.ssh/"   
        echo "copying init.sh"
        scp init.sh cc@$i:"~/"  
        echo "copying java.security"
        scp files/java.security cc@$i:"~/"  
        echo "copying pom.xml"
        scp files/pom.xml cc@$i:"~/"  
       '
       #echo "copying config file"
       #scp config.properties cc@$i:"/home/cc/bzs/java/bzstore/scripts/"
       #echo "copying data.txt"
       #scp data.txt cc@$i:"/home/cc/bzs/java/bzstore/scripts/"
       scp tinit.sh cc@$i:"~/"
      done 
    elif [[ "$3" == "init" ]]
    then
      echo "running init.sh"
      run_command_on_all_nodes "./tinit.sh"
    elif [[ "$3" == "config" ]]
    then
      echo "configuring node ips"
      update_replica_ips
      update_config 
      j=0
      for i in  `cat $clusterIPFile` ;
      do
        echo "replica $j private ip: ${replica_ips[$j]} public ip: $i"
      	scp hosts.config cc@$i:"$wdb_home/config/"
      	sed "s/= auto/= ${replica_ips[$j]}/" bftsmart_conf/system.config.bak > system.config
      	scp system.config cc@$i:"$wdb_home/config/"  
      	#scp system.config cc@${replica_floating_ips[$j]}:"$wdb_home/config/"  
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
elif [[ "$2" == "clearip" ]]
then
	clear_floating_ips 
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
