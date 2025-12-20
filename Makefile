gen-proto:
	protoc --go_out=. --go-grpc_out=. --plugin=protoc-gen-go=/Users/stepa/go/bin/protoc-gen-go --plugin=protoc-gen-go-grpc=/Users/stepa/go/bin/protoc-gen-go-grpc protos/raft.proto

run:
	go run main.go

lint:
	golangci-lint run