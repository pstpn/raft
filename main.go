package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // debug endpoint
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

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
	logger.Infof("fsm data directory: %q", cfg.FSMDataDir)

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

	tcp, err := net.Listen("tcp", cfg.NodeAddr)
	if err != nil {
		logger.Errorf("failed to listen on %q: %v", cfg.NodeAddr, err)
		return
	}
	defer tcp.Close()

	pprofServer := http.Server{Addr: cfg.PprofAddr, ReadHeaderTimeout: time.Second}
	grpcServer := grpc.NewServer()
	protos.RegisterRaftServer(grpcServer, NewGRPCServer(raft))
	reflection.Register(grpcServer)

	gr.Go(func() error {
		if err := raft.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Errorf("raft worker stopped with error: %v", err)
			return err
		}

		logger.Info("raft worker stopped")
		return nil
	})
	gr.Go(func() error {
		logger.Infof("starting pprof server on %q", cfg.PprofAddr)

		if err := pprofServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Errorf("pprof server stopped with error: %v", err)
			return err
		}

		logger.Info("pprof server stopped")
		return nil
	})
	gr.Go(func() error {
		logger.Infof("starting raft node on %q (cluster nodes: %q)", cfg.NodeAddr, cfg.ClusterNodesAddr)

		if err := grpcServer.Serve(tcp); err != nil {
			logger.Errorf("raft server stopped with error: %v", err)
			return err
		}

		logger.Info("raft server stopped")
		return nil
	})

	gr.Go(func() error {
		<-ctx.Done()
		grpcServer.GracefulStop()
		return pprofServer.Shutdown(ctx)
	})

	if err := gr.Wait(); err != nil {
		logger.Error(err.Error())
		return
	}
}
