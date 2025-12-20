package main

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	grpcserver "raft/grpc"
	"raft/protos"
)

func main() {
	grpcServer := grpc.NewServer()
	grpcHandler := grpcserver.NewServer()
	protos.RegisterRaftServer(grpcServer, grpcHandler)

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", 4044))
	if err != nil {
		panic(err)
	}

	err = grpcServer.Serve(l)
	if err != nil {
		panic(err)
	}
}
