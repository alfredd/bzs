#!/usr/bin/env bash

if [ "$1" == "start" ]; then
  echo "Starting WedgeDB cluster."
  shift
  echo "Cluster number is $1"
  for i in {0..3}; do
    echo "./bzs.sh $1 $i &"
    ./bzs.sh $1 $i &>"c_$1_$i.log" &
  done
elif [ "$1" == "stop" ]; then
  echo "Stopping all WedgeDB clusters."
#  for i in $(ps -eaf | grep BZStoreServer | awk '{ print $2 }'); do
#    ps -eaf | grep "$i"
#    if [[ "$?" == "0" ]]; then
#      kill $i
#    fi
#  done
  for i in $(ps -eaf | grep -i bzs | awk '{ print $2 }'); do
    sleep 1
    ps -eaf | grep "$i"
    if [[ "$?" == "0" ]]; then
      kill -9 $i
      echo "Killing $i"
    fi
  done
elif [ "$1" == "clean" ]; then
  echo "Removing db files."
  rm BZS_data_* -rf
else
  echo "Invalid option"
  echo "Usage: "
  echo "      $0 start [NUM] | stop | clean"
fi
