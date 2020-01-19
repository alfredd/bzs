#!/bin/bash

results_dir="results"
projectFile="PEACHes-openrc.sh"
keyname=chameleonkey
network=sharednet1
replicaReservationid=0061491b-7101-4b45-8dcc-c23f30fceb68
clientReservationid=dd52f289-16e5-49e6-9554-0040832f88c2

clusterNumber=0

numNodes=4
#numNodes=6


NO_ARGS=0 
if [ $# -eq "$NO_ARGS" ]    # Script invoked with no command-line args?
then
  echo "Usage: `basename $0` options (-ac:i:s:k:fgl)"
cat <<usage
a creates aliases for replicas and clients for ease of logging in 
c [replicas|clients] will create instances of replicas or clients. Modify $numNodes if number of replicas and clients differ
i [replicas|clients] will set up the replicas/clients with the bftsmart library and build it
s [replicas|clients] will start up the replicas or clients  
k [replicas|clients] will kill the bftsmart processes on the replicas and clients
f will clear the floating ips associated with the instances 
g will gather the experimental results from the replicas into the results directory 
e given reservations for client and replicas, it will create and setup all nodes for experiment 
l will perform a series of experiment with varying number of clients 
usage
  exit $E_OPTERROR          
fi


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
    cp bftsmart_conf/hosts.config.stub hosts.config 
  fi

  for i in "${!replica_ips[@]}"; do
    echo "$i ${replica_ips[$i]} 11000 11001" >> hosts.config 
  done
  
  echo "" >> hosts.config 
  echo $ttp_ip >> hosts.config 
}


#launch ubuntu instances on chameleon
function create_instances {
	nodeType=$1

  if [ "$nodeType" == "r" ]
  then
    nodeReservation=$replicaReservationid 
  else 
    nodeReservation=$clientReservationid 
  fi
  
	seqEnd=$((numNodes-1))
	for i in $(seq 0 $seqEnd)
	do
		nodeName=$nodeType$i
  	openstack server create \
			--image CC-Ubuntu16.04 \
			--flavor baremetal \
			--key-name chameleonkey \
			--nic net-id=sharednet1 \
			--hint reservation=$nodeReservation \
			--user-data scripts/init.sh \
			$nodeName 
    sleep 10 #sleeps for a few seconds between calls
	done 
}


#kill bft-smart processes on the nodes
function kill_processes {
	ip_arr=("$@")

	for i in "${!ip_arr[@]}"; do
    scp scripts/killprocesses.sh cc@${ip_arr[$i]}:~/
    ssh cc@${ip_arr[$i]} "./killprocesses.sh"
	done
}



#start up the replica processes for benchmarking
function start_replicas {
  for i in "${!replica_floating_ips[@]}"; do
    ssh cc@${replica_floating_ips[$i]} "cd library; rm config/currentView; ant; java -Djava.security.properties="./config/java.security" -Dlogback.configurationFile="./config/logback.xml" -cp bin/*:lib/* bftsmart.demo.microbenchmarks.ThroughputLatencyServer $i 1200 100 10 0 \"nosig\" &> $i.txt &"
  done 
}


#start up the client processes, first copying the runclient.sh script over
function start_clients {
  totalThreads=$1
  echo "start_clients totalThreads: $totalThreads"
  for i in "${!client_floating_ips[@]}"; 
  do
    echo $i
    scp scripts/runclient.sh cc@${client_floating_ips[$i]}:~/library/
    ssh cc@${client_floating_ips[$i]} "cd library; ant; chmod +x runclient.sh; ./runclient.sh $i $totalThreads"
  done 
}


#initialize bft-smart at the replicas with custom configurations
function initialize_replicas {
  update_replica_ips 
  for i in "${!replica_ips[@]}"; do 
      #ssh cc@${replica_floating_ips[$i]} "git clone https://github.com/bft-smart/library.git"
      ssh cc@${replica_floating_ips[$i]} "rm -rf library; git clone https://github.com/KohdMonkey/library.git; cd library; git checkout speculative"
      sed "s/= auto/= ${replica_ips[$i]}/" bftsmart_conf/system.config.bak > system.config 
      scp system.config cc@${replica_floating_ips[$i]}:~/library/config/
      scp hosts.config cc@${replica_floating_ips[$i]}:~/library/config/
      scp bftsmart_conf/java.security cc@${replica_floating_ips[$i]}:~/
      ssh cc@${replica_floating_ips[$i]} "sudo cp java.security /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/security/"
    done
}


#intiliaze bft-smart at the clients 
function initialize_clients {
 for i in "${!client_floating_ips[@]}"; do 
    #ssh cc@${client_floating_ips[$i]} "git clone https://github.com/bft-smart/library.git"
    ssh cc@${client_floating_ips[$i]} "rm -rf library; git clone https://github.com/KohdMonkey/library.git; cd library; git checkout speculative"
    scp bftsmart_conf/java.security cc@${client_floating_ips[$i]}:~/ 
    ssh cc@${client_floating_ips[$i]} "sudo cp java.security /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/security/"
    scp hosts.config cc@${client_floating_ips[$i]}:~/library/config/
  done 
}

#clear the floating ips
function clear_floating_ips {
	for i in "${!client_floating_ips[@]}"; do
		ssh-keygen -f "/home/kohdmonkey/.ssh/known_hosts" -R ${client_floating_ips[$i]}
	done

	for i in "${!replica_floating_ips[@]}"; do
		ssh-keygen -f "/home/kohdmonkey/.ssh/known_hosts" -R ${replica_floating_ips[$i]}
	done
}


function add_floating_ips {
	for i in "${!client_floating_ips[@]}"; do
    client=c$i
    echo "adding ip for client $client"
		openstack server add floating ip $client ${client_floating_ips[$i]}
	done

	for i in "${!replica_floating_ips[@]}"; do
    replica=r$i
    echo "adding ip for replica $replica"
		openstack server add floating ip $replica ${replica_floating_ips[$i]}
	done
}


#gathering results from the replicas into directory
function gather_results {
  for i in "${!replica_floating_ips[@]}"; do
    scp cc@${replica_floating_ips[$i]}:~/library/$i.txt results/
  done
}


#make aliases for accessing replicas and clients
function make_aliases {
	node=$1 && shift
	ip_arr=("$@")

	for i in "${!ip_arr[@]}"; do
		alias_name=go$node$i
    echo $alias_name 
    echo alias $alias_name=\""ssh cc@${ip_arr[$i]}\"" >> aliases 	
	done
}


function large_experiments {
  numClients=200

  for  ((clients=$numClients, j=0; j<13; j++))
  #for  ((clients=$numClients, j=0; j<8; j++))
  do
    echo "running experiments with $clients clients"
    echo "starting replicas"
    start_replicas 
    sleep 20s
    echo "starting clients"
    start_clients "$clients"

    echo "sleeping for 60 seconds while results finishes"
    sleep 60 #sleeps for 60 seconds while experiment finishes
    gather_results 

    echo "gathering results"
    cd results 
    ./process_output.sh "$clients"  
    cd .. 

    echo "killing processes"
    kill_processes "${replica_floating_ips[@]}"
    kill_processes "${client_floating_ips[@]}"

    (( clients = clients + $numClients ))
  done
}


source ./nodes.sh  #retrieve the latest node configurations

while getopts "z:ac:s:i:k:fgel" option
do
  case ${option} in
  z) 
      clusterNumber=$OPTARG 
      echo "cluster number: $clusterNumber"
  ;;
  a)  echo "creating aliases"
			if [ -f "aliases" ]
			then
				echo "removing old aliases file"
				rm ./aliases 
			fi 
			make_aliases r "${replica_floating_ips[@]}"
			make_aliases c "${client_floating_ips[@]}"
	;;
 	c)  target=$OPTARG
		  echo "creating $target instances"
			if [ "$target" == "replicas" ]
			then
				#create_instances r
        echo "creating replicas for cluster $clusterNumber"
			elif [ "$target" == "clients" ]
			then
        echo "creating clients"
				#create_instances c
			else
				echo "invalid parameter"
				exit 
			fi
	;;
	i)  target=$OPTARG
		  echo "initializing $target for experiment"
			#first reinitialize the hosts.config file
			if [ "$target" == "replicas" ]
			then
				initialize_replicas
			elif [ "$target" == "clients" ]
			then
				initialize_clients 
			else
				echo "invalid parameter"
				exit 
			fi
	;;
 	s)  target=$OPTARG
		  echo "starting $target instances"
			if [ "$target" == "replicas" ]
			then
				start_replicas 
			elif [ "$target" == "clients" ]
			then
				start_clients  
			else
				echo "invalid parameter"
				exit 
			fi
	;;
	k)  target=$OPTARG
		  echo "killing $target processes"
			if [ "$target" == "replicas" ]
			then
				kill_processes "${replica_floating_ips[@]}"
			elif [ "$target" == "clients" ]
			then
				kill_processes "${client_floating_ips[@]}"
			else
				echo "invalid parameter"
				exit 
			fi
	;;
	f) 
			echo "updating ips and config file"
			clear_floating_ips  
      add_floating_ips 
      update_replica_ips 
		  update_config 
	;;
	g)  echo "gathering results into $results_dir"
  		gather_results
	;;  
	e)  echo "setting up all replicas and clients given their respective reservation"
      ssh-add ./chameleonkey.pem  #add the api key
      source $projectFile         #chameleon api access 
      create_instances r          #launch replica instances
      create_instances c           #launch client instances
			clear_floating_ips          #clear floating ips of replicas and clients
      add_floating_ips 
      echo "sleeping for a few minutes while nodes are spawning"
      sleep 15m                   #sleep for a bit until nodes are done spawning
      echo "done sleeping, continuing setup"
      update_replica_ips          #update replica ips for node-to-node comm
			update_config               #update the bft-smart config file
			make_aliases                #make aliases for easy ssh
			initialize_replicas         #initialize replicas with bft-smart 
			initialize_clients          #intialize clients with bft-smart
	;;
	l)  echo "running large experiments"
  		large_experiments 
	;;  
  esac
done

