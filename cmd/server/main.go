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

	log.Println("Starting CalCard server...")
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

	stor := store.New(dbManager.DB)
	sessionManager := appauth.NewSessionManager(cfg, stor)
	authService, err := appauth.NewService(cfg, stor, sessionManager)
	if err != nil {
		return fmt.Errorf("failed to initialize auth service: %w", err)
	}

	go store.StartLockCleanup(ctx, stor.Locks, 5*time.Minute)

	r := httpserver.NewRouterWithOptions(cfg, stor, authService, opts.Router)

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("server listening on %s", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
	return nil
}
