gen-proto:
	protoc --go_out=. --go-grpc_out=. protos/raft.proto

run:
	go run main.go

lint:
	golangci-lint run