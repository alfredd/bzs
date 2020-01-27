#!/bin/bash
#bash script to initialize each node

#stop resource monitoring
systemctl stop collectd.service
systemctl disable collectd.service
systemctl stop os-collect-config.service
systemctl disable os-collect-config.service

#install necessary packages
sudo apt-get update
sudo apt-get install -y ant

git clone https://github.com/bft-smart/library.git
