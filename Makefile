gen-proto:
	protoc --go_out=. --go-grpc_out=. --plugin=protoc-gen-go=/Users/stepa/go/bin/protoc-gen-go --plugin=protoc-gen-go-grpc=/Users/stepa/go/bin/protoc-gen-go-grpc protos/raft.proto

build-local:
	go build -o raft .

build:
	docker build -t github.com/pstpn/raft .

run-local: build-local
	./raft

up-cluster: build
	docker compose up -d

down-cluster:
	-docker compose down

rm-cluster: down-cluster
	-docker image rm github.com/pstpn/raft:latest
	-docker volume rm raft_cluster_raft_node_1_data raft_cluster_raft_node_2_data raft_cluster_raft_node_3_data

lint:
	golangci-lint run