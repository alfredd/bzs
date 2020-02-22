#!/usr/bin/env bash

key=""
user=""
wdb_home=""
num_aws_clusters=1

function setvars() {
  cluster=$1
  if (( $cluster < $num_aws_clusters )); then
    key="aws_key"
    user="ubuntu"
		wdb_home="/home/ubuntu/bzs/java/bzstore/scripts"
  else
    key="mykey"
    user="cc"
		wdb_home="/home/cc/bzs/java/bzstore/scripts"
  fi  
}


function update_repo_on_all_nodes() {
  START=0
  END=$1
  for ((c = $START; c < $END; c++)); do
    ./db.sh "$c" uprepo
  done
}


function clear_db_and_working_directories() {
  START=0
  END=$1
  for ((c = $START; c < $END; c++)); do
		setvars $c 
    echo "clearing db with key $key and $user at $wdb_home"
    ./db.sh "$c" bftclear $key $user $wdb_home  
    ./db.sh "$c" clean $key $user $wdb_home  
  done
}

function build_code() {
  START=0
  END=$1
  for ((c = $START; c < $END; c++)); do
    ./db.sh "$c" build
  done
}

function stop_all_clusters() {
  START=0
  END=$1
  for ((c = $START; c < $END; c++)); do
		setvars $c 
    echo "stopping all clusters with key $key and $user at $wdb_home"
    ./db.sh "$c" stop $key $user $wdb_home 
  done
}

function start_all_clusters() {
  START=1
  END=$1
  setvars 0
  ./db.sh "0" starty $wdb_home 
  for ((c = $START; c < $END; c++)); do
		setvars $c 
    echo "starting clusters with key $key and $user at $wdb_home"
    ./db.sh "$c" start $key $user $wdb_home   
  done
}

function get_logs_from_all_clusters() {
  START=0
  END=$1
  for ((c = $START; c < $END; c++)); do
		setvars $c 
    echo "getting logs with key $key and $user at $wdb_home"
    ./db.sh "$c" logall $key $user $wdb_home  
  done
}

function copy_data_to_all_nodes() {
  START=0
  END=$1
  for ((c = $START; c < $END; c++)); do
		setvars $c 
    echo "copying data with key $key and $user at $wdb_home"
    ./db.sh "$c" copydb $key $user $wdb_home 
  done
}

function create_nkey_data() {
  keysize=$1
  START=1
  END=$keysize
  rm "data.txt"
  for ((c = $START; c <= $END; c++)); do 
    echo $c >>"data.txt"
  done
}

cluster_count=$1
key_count=$2

#echo "Running benchmark for 76 Transactions per batch. R:W ratio of 5:1"
#stop_all_clusters $cluster_count
#sleep 3

if [[ "$3" == "build" ]]; then
  echo "Rebuildng code on all clusters"
  update_repo_on_all_nodes $cluster_count
  build_code $cluster_count
  sleep 45
fi
sleep 1

:'
create_nkey_data "$key_count"

copy_data_to_all_nodes "$cluster_count"

config_files=($(ls test_configurations_and_data/))
S=0
E=$cluster_count
for ((c = $S; c < $E; )); do
  echo "Clearing old db and BFT-SMaRt state."
  i=${config_files[$c]}
  clear_db_and_working_directories $cluster_count
  sleep 3
  batch_size=$(echo $i | cut -d'_' -f 2)
  rd_ratio=$(echo $i | cut -d'_' -f 3)
  wr_ratio=$(echo $i | cut -d'_' -f 4)
  config_file="test_configurations_and_data/$i"
  echo "Running benchmark for config file: $config_file"
  cp "$config_file" ./config.properties

  echo "Starting all clusters for benchmark run."
  start_all_clusters $cluster_count

  echo "Waiting for benchmark run to complete for batch size: $batch_size, rd ratio: $rd_ratio, wr ratio: $wr_ratio"
  found=1
  try_again=0
  while [[ $found -eq 1 ]]; do
    sleep 60
    grep "END OF BENCHMARK RUN" db.log
    if [[ "$?" == "0" ]]; then
      echo "Benchmark completed for batch size: $batch_size, rd ratio: $rd_ratio, wr ratio: $wr_ratio"
      found=0
      c=$((c + 1))

    else
      echo "Checking if DEADLINE_EXCEEDED"
      grep "DEADLINE_EXCEEDED" db.log
      if [[ "$?" == "0" ]]; then
        echo "Benchmark failed with exception 'DEADLINE_EXCEEDED' for batch size: $batch_size, rd ratio: $rd_ratio, wr ratio: $wr_ratio"
        found=0
        if [[ "$try_again" == "1" ]]; then
          echo "Benchmark failed AGAIN with exception 'DEADLINE_EXCEEDED' for batch size: $batch_size, rd ratio: $rd_ratio, wr ratio: $wr_ratio"
          echo "Will skip to next run."
          c=$((c + 1))
        else
          try_again=1
          echo "Trying one more time."
        fi
      fi
    fi

  done

  get_logs_from_all_clusters $cluster_count
  benchmark_log_directory="logs-$keysize-$batch_size-$rd_ratio-$wr_ratio"
  mkdir -p "$benchmark_log_directory"
  mv logs/* "$benchmark_log_directory/"

  echo "Stopping all clusters"
  stop_all_clusters $cluster_count
  sleep 1
done
'
#copy_data_to_all_nodes "$cluster_count"
#clear_db_and_working_directories $cluster_count
#start_all_clusters $cluster_count

#get_logs_from_all_clusters $cluster_count
stop_all_clusters $cluster_count



