package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"
	_ "time/tzdata"

	appauth "github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	httpserver "github.com/jw6ventures/calcard/internal/http"
	"github.com/jw6ventures/calcard/internal/logging"
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
	if cfg.TrafficCaptureFile != "" && opts.Router.TrafficCaptureWriter == nil {
		captureFile, err := openTrafficCaptureFile(cfg.TrafficCaptureFile)
		if err != nil {
			return fmt.Errorf("failed to open traffic capture file: %w", err)
		}
		defer func() {
			if err := captureFile.Close(); err != nil {
				jw6utils.Log("Main", "runServer", jw6_utils.Error, fmt.Sprintf("failed to close traffic capture file: %v", err))
			}
		}()
		opts.Router.TrafficCaptureWriter = captureFile
		jw6utils.Log("Main", "runServer", jw6_utils.Warn, fmt.Sprintf("request traffic capture enabled: %s", cfg.TrafficCaptureFile))
	}

	logInsecureBasicWarning(&jw6utils, cfg)
	logPerReplicaLimits(&jw6utils, cfg)

	relayPostgresNotices(&jw6utils)
	dbManager := database.NewManager(database.Config{
		Driver:           postgresDriverName,
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
	authService.SetLogger(&jw6utils)

	// With Digest off, clears any HA1 an earlier run stored; repeating it
	// retries a failed pass without waiting for a restart.
	go store.StartDigestCredentialPurge(ctx, stor.AppPasswords, appauth.DigestEnabled(cfg), time.Hour)
	go store.StartLockCleanup(ctx, stor.Locks, 5*time.Minute)
	// Every authenticated Digest request writes one nonce count, and each stops
	// being presentable one nonce lifetime after it was written.
	go store.StartDigestNonceCleanup(ctx, stor.DigestNonces, 5*time.Minute)
	// The retention window is measured in weeks, so an hourly pass keeps the
	// table bounded without the prune ever being the reason a query is slow.
	go store.StartDeletedResourceCleanup(ctx, stor.DeletedResources, time.Hour, cfg.DAV.SyncHistoryRetention)

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
		pprofSrv := startPprofServer(cfg.PprofAddr, &jw6utils)
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

// logInsecureBasicWarning reports on every start that Basic is accepted over
// plain HTTP, which RFC 4791 Section 11 forbids: the app password and all
// calendar and contact data then cross the network in cleartext.
func logInsecureBasicWarning(sink logging.Sink, cfg *config.Config) {
	if !cfg.DAV.InsecureBasicEnabled {
		return
	}
	logging.New(sink, "Main").Warn("runServer", "APP_DAV_INSECURE_BASIC_ENABLED is set: HTTP Basic is accepted on /dav and /api over plain HTTP, so app passwords and all calendar and contact data cross the network in cleartext. RFC 4791 Section 11 forbids Basic without TLS; terminate TLS in front of CalCard and unset it where you can")
}

// logPerReplicaLimits states the database pool and rate limits this process
// enforces. Neither is shared between replicas, so a fleet's connection count
// and the rate it admits from one client are these figures times the replica
// count.
func logPerReplicaLimits(sink logging.Sink, cfg *config.Config) {
	maxOpen := "unlimited"
	if cfg.DB.MaxOpenConns > 0 {
		maxOpen = strconv.Itoa(cfg.DB.MaxOpenConns)
	}
	logging.New(sink, "Main").Info("runServer",
		"limits per replica (each replica enforces its own; multiply by the replica count for the fleet): "+
			"database pool max open %s, max idle %d, conn max lifetime %s; "+
			"rate limits per client IP: auth %s, DAV and API %s",
		maxOpen, cfg.DB.MaxIdleConns, cfg.DB.ConnMaxLifetime,
		describeRateLimit(cfg.RateLimit.AuthRPS, cfg.RateLimit.AuthBurst), describeRateLimit(cfg.RateLimit.DAVRPS, cfg.RateLimit.DAVBurst))
}

// describeRateLimit spells one limiter for the startup line; a rate of 0 is a
// limiter turned off.
func describeRateLimit(rps float64, burst int) string {
	if rps <= 0 {
		return "off"
	}
	return fmt.Sprintf("%g req/s burst %d", rps, burst)
}

func openTrafficCaptureFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}
