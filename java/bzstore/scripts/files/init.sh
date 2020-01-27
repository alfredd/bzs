#!/bin/bash

wdb_home="/home/cc/bzs/java/bzstore/scripts"

eval `ssh-agent -s`
ssh-add .ssh/github_rsa

echo "cloning bzs repo"
git clone --recurse-submodules git@github.com:alfredd/bzs.git

echo "installing maven"
sudo apt-get install -y maven



cd $wdb_home
./bzs-setup.sh bftsmart
#./bzs-setup.sh install 


