package main

import (
	"context"
	"fmt"
	"github.com/alfredd/bzs/protocol"
	"google.golang.org/grpc"
	"log"
	"net"
)

type BZServer struct{}

func (bzs *BZServer) Commit(ctx context.Context, req *protocol.TransactionRequest) (*protocol.TransactionResponse, error) {
	log.Println("Received commit request.")
	response := new(protocol.TransactionResponse)
	response.Id = req.Id
	response.Key = req.Key
	response.Value = req.Value
	response.Version = req.Version + 1
	return response, nil
}

func (bzs *BZServer) Read(ctx context.Context, req *protocol.TransactionRequest) (*protocol.TransactionResponse, error) {
	log.Println("Received read request.")
	response := new(protocol.TransactionResponse)
	response.Id = req.Id
	response.Key = req.Key
	response.Value = req.Value
	response.Version = req.Version + 1
	return response, nil
}

func main() {
	log.Println("Starting bzs server")
	port := 4550
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("Error in listening to port: %d\n", port)
	}
	server := grpc.NewServer()
	protocol.RegisterTransactionServer(server, &BZServer{})
	server.Serve(lis)
}
