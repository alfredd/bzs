#!/usr/bin/env bash

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
    ./db.sh "$c" bftclear
    ./db.sh "$c" clean
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
    ./db.sh "$c" stop
  done
}

function start_all_clusters() {
  START=1
  END=$1
  ./db.sh "0" starty
  for ((c = $START; c < $END; c++)); do
    ./db.sh "$c" start
  done
}

function get_logs_from_all_clusters() {
  START=0
  END=$1
  for ((c = $START; c < $END; c++)); do
    ./db.sh "$c" logall
  done
}

function copy_data_to_all_nodes() {
  START=0
  END=$1
  for ((c = $START; c < $END; c++)); do
    ./db.sh "$c" copydb
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

echo "Running benchmark for 76 Transactions per batch. R:W ratio of 5:1"
stop_all_clusters $cluster_count
sleep 3

if [[ "$3" == "build" ]]; then
  echo "Rebuildng code on all clusters"
  update_repo_on_all_nodes $cluster_count
  build_code $cluster_count
  sleep 45
fi
sleep 1
create_nkey_data "$key_count"

copy_data_to_all_nodes "$cluster_count"

config_files=($(ls test_configurations_and_data/))
S=0
E=$cluster_count
for ((c = $s; c < $E;)); do
  echo "Clearing old db and BFT-SMaRt state."
  clear_db_and_working_directories $cluster_count
  sleep 3

  batch_size=`echo $i | cut -d'_' -f 2`
  rd_ratio=`echo $i | cut -d'_' -f 3`
  wr_ratio=`echo $i | cut -d'_' -f 4`
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
    if [[ "$?"  == "0" ]]; then
      echo "Benchmark completed for batch size: $batch_size, rd ratio: $rd_ratio, wr ratio: $wr_ratio"
      found=0;
      c=$((c+1))
    fi
    grep "DEADLINE_EXCEEDED" db.log
    if [[ "$?"  == "0" ]]; then
      echo "Benchmark failed with exception 'DEADLINE_EXCEEDED' for batch size: $batch_size, rd ratio: $rd_ratio, wr ratio: $wr_ratio"
      found=0;
      if [[ "$try_again" == "1" ]]; then
        echo "Benchmark failed AGAIN with exception 'DEADLINE_EXCEEDED' for batch size: $batch_size, rd ratio: $rd_ratio, wr ratio: $wr_ratio"
        echo "Will skip to next run."
        c=$((c+1))
      else
        try_again=1
        echo "Trying one more time."
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
