package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bogdanaks/yougpu-agent/internal/agent"
	"github.com/bogdanaks/yougpu-agent/internal/client"
	"github.com/bogdanaks/yougpu-agent/internal/config"
	"github.com/bogdanaks/yougpu-agent/internal/container"
	"github.com/bogdanaks/yougpu-agent/internal/disk"
	"github.com/bogdanaks/yougpu-agent/internal/firewall"
	"github.com/bogdanaks/yougpu-agent/internal/hostsetup"
	"github.com/bogdanaks/yougpu-agent/internal/lifecycle"
	"github.com/bogdanaks/yougpu-agent/internal/sts"
	"github.com/bogdanaks/yougpu-agent/internal/system"
)

var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("agent starting", "version", version)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config load failed", "err", err)
		os.Exit(1)
	}

	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("signal received, shutting down", "signal", sig.String())
		cancel()
	}()

	httpClient := client.New(cfg.BackendURL, cfg.Token, version, logger)
	executor := system.NewExecutor(logger)
	systemd := system.NewSystemd(executor, logger)
	diskMgr := disk.NewManager(systemd, executor, logger)
	diskMgr.SetRcPortBase(cfg.RcloneRcPortBase)
	if cfg.DiskDriver == config.DiskDriverDirect {
		diskMgr.SetDirectMode(true)
		logger.Info("disk driver: direct (rclone --daemon, без systemd)")
	}
	containerMgr := container.NewManager(executor, container.NewSocketPuller(), logger)
	firewallMgr := firewall.NewManager(executor, logger)
	hostSetupMgr := hostsetup.NewManager(executor, systemd, logger)
	lifecycleMgr := lifecycle.NewManager(cfg.StateDir, systemd, executor, logger)
	credsProvider := sts.NewProvider(httpClient, diskMgr, logger, cfg.CredsRefreshThreshold, cfg.CredsPeriodicInterval)

	a := agent.New(agent.Config{
		Version:           version,
		PollInterval:      15 * time.Second,
		HeartbeatInterval: cfg.HeartbeatInterval,
		ReconcileInterval: cfg.ReconcileInterval,
		Client:            httpClient,
		Disk:              diskMgr,
		Container:         containerMgr,
		Firewall:          firewallMgr,
		HostSetup:         hostSetupMgr,
		Lifecycle:         lifecycleMgr,
		Creds:             credsProvider,
		Logger:            logger,
	})

	if err := a.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("agent terminated", "err", err)
		os.Exit(1)
	}

	logger.Info("agent stopped")
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
