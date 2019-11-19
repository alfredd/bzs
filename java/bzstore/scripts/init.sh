#!/bin/bash

wdb_home="/home/cc/bzs/java/bzstore/scripts"

eval `ssh-agent -s`
ssh-add .ssh/github_rsa

echo "cloning bzs repo"
git clone --recurse-submodules git@github.com:alfredd/bzs.git

echo "installing maven"
sudo apt-get install -y maven

#echo "retrieving oracle JDK"
#wget --no-check-certificate -c --header "Cookie: oraclelicense=accept-securebackup-cookie" https://download.oracle.com/otn-pub/java/jdk/13.0.1+9/cec27d702aa74d5a8630c65ae61e4305/jdk-13.0.1_linux-x64_bin.tar.gz

sudo mkdir /opt/jdk
sudo tar -zxf jdk-10.0.2_linux-x64_bin.tar.gz -C /opt/jdk

export JAVA_HOME=/opt/jdk/jdk-10.0.2
export PATH=$PATH:$JAVA_HOME/bin

sudo update-alternatives --install /usr/bin/java java /opt/jdk/jdk-10.0.2/bin/java 100
sudo update-alternatives --install /usr/bin/javac javac /opt/jdk/jdk-10.0.2/bin/javac 100
sudo update-alternatives --set java /opt/jdk/jdk-10.0.2/bin/java
sudo update-alternatives --set javac /opt/jdk/jdk-10.0.2/bin/javac

echo "checking java version"
java -version

cd $wdb_home
./bzs-setup.sh bftsmart

echo "installing protocol buffer"
cd 
sudo apt-get install autoconf automake libtool curl make g++ unzip
git clone https://github.com/protocolbuffers/protobuf.git
cd protobuf
git submodule update --init --recursive
./autogen.sh 
./configure
make
make check
sudo make install
sudo ldconfig # refresh shared library cache.

cd
cd $wdb_home
./bzs-setup install 


