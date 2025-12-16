package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	jw6_utils "gitea.jw6.us/Utilities/jw6-go-utils"
	appauth "gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/config"
	httpserver "gitea.jw6.us/james/calcard/internal/http"
	"gitea.jw6.us/james/calcard/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

const version = "v0.1.0"

func main() {
	logLevelString := os.Getenv("LOG_LEVEL")
	if logLevelString == "" {
		logLevelString = "Info"
	}
	logLevel := jw6_utils.LogLevelFromString(logLevelString)
	// Initialize utilities
	jw6utils := jw6_utils.Utils{LogLevel: logLevel}
	jw6utils.PrintBanner("CalCard", version, "2025", 3, "James Williams")

	log.Println("Starting CalCard server...")
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DB.DSN)
	if err != nil {
		log.Fatalf("failed to create db pool: %v", err)
	}
	defer pool.Close()

	if err := store.ApplyMigrations(ctx, pool); err != nil {
		log.Fatalf("failed to apply migrations: %v", err)
	}

	stor := store.New(pool)
	sessionManager := appauth.NewSessionManager(cfg, stor)
	authService, err := appauth.NewService(cfg, stor, sessionManager)
	if err != nil {
		log.Fatalf("failed to initialize auth service: %v", err)
	}

	r := httpserver.NewRouter(cfg, stor, authService)

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
}
