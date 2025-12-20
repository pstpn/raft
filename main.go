package main

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"raft/protos"
)

func main() {
	// ctx := context.Background()

	// raft, err := NewRaft(ctx, )
	// if err != nil {
	//	// log.Fatal
	//	panic(err)
	//}

	grpcHandler := NewGRPCServer(nil)
	grpcServer := grpc.NewServer()
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
