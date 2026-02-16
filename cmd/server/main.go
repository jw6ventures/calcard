package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	appauth "github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	httpserver "github.com/jw6ventures/calcard/internal/http"
	"github.com/jw6ventures/calcard/internal/store"
	jw6_utils "github.com/jw6ventures/jw6-go-utils"
	"github.com/jw6ventures/jw6-go-utils/database"
)

const version = "v1.0.7"

func main() {
	logLevelString := os.Getenv("LOG_LEVEL")
	if logLevelString == "" {
		logLevelString = "Info"
	}
	logLevel := jw6_utils.LogLevelFromString(logLevelString)

	jw6utils := jw6_utils.Utils{LogLevel: logLevel}
	jw6utils.PrintBanner("CalCard", version, "2026", 3, "JW6 Ventures LLC")

	log.Println("Starting CalCard server...")
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
		log.Fatalf("failed to initialize database: %v", err)
	}
	defer dbManager.Close()

	stor := store.New(dbManager.DB)
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
