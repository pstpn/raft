package main

import (
	"context"
	"errors"
	"net"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"raft/protos"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer stop()
	gr, ctx := errgroup.WithContext(ctx)
	cfg := ReadConfig()
	logger := NewSimpleLogger(cfg.LogLevel)

	fsm, err := NewSimpleFSM(cfg.FSMDataDir)
	if err != nil {
		logger.Errorf("failed to create fsm: %v", err)
		return
	}
	logger.Infof("fsm data directory: %s", cfg.FSMDataDir)

	persistent, err := NewSimplePersistent(cfg.PersistentDataDir)
	if err != nil {
		logger.Errorf("failed to create persistent storage: %v", err)
		return
	}
	logger.Infof("persistent data directory: %q", cfg.PersistentDataDir)

	raft, err := NewRaft(ctx, cfg, logger, fsm, persistent)
	if err != nil {
		logger.Errorf("failed to create raft worker: %v", err)
		return
	}

	grpcHandler := NewGRPCServer(raft)
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()
	protos.RegisterRaftServer(grpcServer, grpcHandler)

	tcp, err := net.Listen("tcp", cfg.NodeAddr)
	if err != nil {
		logger.Errorf("failed to listen on %q: %v", cfg.NodeAddr, err)
		return
	}
	defer tcp.Close()

	gr.Go(func() error {
		if err := raft.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Errorf("raft worker stopped with error: %v", err)
			return err
		}

		logger.Info("raft worker stopped")
		return nil
	})
	gr.Go(func() error {
		logger.Infof("starting raft node on %q", cfg.NodeAddr)
		logger.Infof("cluster nodes %q", cfg.ClusterNodesAddr)

		if err := grpcServer.Serve(tcp); err != nil && !errors.Is(err, context.Canceled) {
			logger.Errorf("raft server stopped with error: %v", err)
			return err
		}

		logger.Info("raft server stopped")
		return nil
	})
	gr.Go(func() error {
		<-ctx.Done()
		grpcServer.Stop()
		return nil
	})

	if err := gr.Wait(); err != nil {
		logger.Error(err.Error())
		return
	}
}
