package main

import (
	"context"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	jw6_utils "github.com/jw6ventures/jw6-go-utils"
)

// startPprofServer starts a dedicated debug listener exposing net/http/pprof.
//
// It is deliberately a separate http.Server rather than handlers mounted on the
// main router: the pprof endpoints expose runtime internals (goroutine stacks,
// heap, command line) and the profile/trace endpoints block for their full
// duration, which makes them trivial denial-of-service vectors. Keeping them on
// a loopback-only listener (see config.PprofAddr) means they are never reachable
// from the public listener; access it in production via an SSH tunnel.
//
// It returns the server so the caller can shut it down on exit. A nil return
// means profiling was not enabled.
func startPprofServer(ctx context.Context, addr string, log *jw6_utils.Utils) *http.Server {
	// Enable contention profiling. Without these, the mutex/block profiles are
	// empty. The sampling rates are light enough to leave on in production.
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(10000)

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
		// ReadHeaderTimeout guards against slow-loris on the debug port. There is
		// deliberately no WriteTimeout: /profile and /trace stream for their full
		// requested duration (e.g. ?seconds=30) and a write deadline would cut
		// them off mid-profile.
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Log("Main", "pprof", jw6_utils.Info, "pprof debug server listening on "+addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Log("Main", "pprof", jw6_utils.Error, "pprof server error: "+err.Error())
		}
	}()

	return srv
}
