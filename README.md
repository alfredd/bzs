

## Commands

__Generate go code for commit__
* protoc -I protocol/ protocol/commit.proto --go_out=plugins=grpc:protocol