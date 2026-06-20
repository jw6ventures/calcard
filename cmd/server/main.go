package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"
	_ "time/tzdata"

	appauth "github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	httpserver "github.com/jw6ventures/calcard/internal/http"
	"github.com/jw6ventures/calcard/internal/store"
	jw6_utils "github.com/jw6ventures/jw6-go-utils"
	"github.com/jw6ventures/jw6-go-utils/database"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthCheck(ctx, os.Getenv("APP_LISTEN_ADDR")); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if err := runServer(ctx, ServerOptions{}); err != nil {
		log.Fatal(err)
	}
}

type ServerOptions struct {
	Router httpserver.RouterOptions
}

func runServer(ctx context.Context, opts ServerOptions) error {
	logLevelString := os.Getenv("LOG_LEVEL")
	if logLevelString == "" {
		logLevelString = "Info"
	}
	logLevel := jw6_utils.LogLevelFromString(logLevelString)

	jw6utils := jw6_utils.Utils{LogLevel: logLevel}
	version := "devel"
	if info, ok := debug.ReadBuildInfo(); ok {
		version = info.Main.Version
	}
	jw6utils.PrintBanner("CalCard", version, "2026", 3, "JW6 Ventures LLC")

	jw6utils.Log("Main", "runServer-mainLoop", jw6_utils.Info, "Starting CalCard server...")
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	dbManager := database.NewManager(database.Config{
		Driver:           "postgres",
		ConnString:       cfg.DB.DSN,
		MigrationsPath:   "migrations",
		AppVersion:       version,
		SchemaPath:       "db.sql",
		SchemaCheckTable: "users",
		Logger:           &jw6utils,
	})
	if err := dbManager.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	defer dbManager.Close()
	dbManager.DB.SetMaxOpenConns(cfg.DB.MaxOpenConns)
	dbManager.DB.SetMaxIdleConns(cfg.DB.MaxIdleConns)
	dbManager.DB.SetConnMaxLifetime(cfg.DB.ConnMaxLifetime)

	store.SetLogger(&jw6utils)

	stor := store.New(dbManager.DB)
	sessionManager := appauth.NewSessionManager(cfg, stor)
	authService, err := appauth.NewService(cfg, stor, sessionManager)
	if err != nil {
		return fmt.Errorf("failed to initialize auth service: %w", err)
	}

	go store.StartLockCleanup(ctx, stor.Locks, 5*time.Minute)

	if opts.Router.Logger == nil {
		opts.Router.Logger = &jw6utils
	}
	r := httpserver.NewRouterWithOptions(cfg, stor, authService, opts.Router)

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      r,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
	}

	if cfg.PprofEnabled {
		pprofSrv := startPprofServer(ctx, cfg.PprofAddr, &jw6utils)
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = pprofSrv.Shutdown(shutdownCtx)
		}()
	}

	go func() {
		jw6utils.Log("Main", "runServer-mainLoop", jw6_utils.Info, fmt.Sprintf("server listening on %s", cfg.ListenAddr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// jw6_utils Fatal does not exit the process, so do it explicitly:
			// a dead listener must surface as a non-zero exit for restart logic.
			jw6utils.Log("Main", "runServer-mainLoop", jw6_utils.Fatal, fmt.Sprintf("server error: %v", err))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	jw6utils.Log("Main", "runServer", jw6_utils.Info, "shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
	return nil
}
