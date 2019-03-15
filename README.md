
# BZStore

* Check code under java/bzstore
* Run bft-smart-setup.sh to setup the configurations
* Currently can be executed via IntellijIDE.

# Todo

* Create scripts that can be used to run the database cluster.



## Commands

__Generate go code for commit__
* protoc -I protocol/ protocol/commit.proto --go_out=plugins=grpc:protocol
