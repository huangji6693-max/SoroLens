// Command sorolens serves the contract explorer: a web UI and a read-only
// JSON API over decoded Soroban contract events.
//
// It runs in one of two modes, selected by SOURCE_MODE:
//
//	rpc        standalone — poll Stellar RPC and store events in Postgres
//	sorotrail  upstream   — read from a SoroTrail indexer's HTTP API
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/sorotrail/sorolens/internal/api"
	"github.com/sorotrail/sorolens/internal/config"
	"github.com/sorotrail/sorolens/internal/decode"
	"github.com/sorotrail/sorolens/internal/ingest"
	"github.com/sorotrail/sorolens/internal/metrics"
	"github.com/sorotrail/sorolens/internal/reqid"
	"github.com/sorotrail/sorolens/internal/rpc"
	"github.com/sorotrail/sorolens/internal/source"
	"github.com/sorotrail/sorolens/internal/source/rpcsource"
	"github.com/sorotrail/sorolens/internal/source/sorotrailsource"
	"github.com/sorotrail/sorolens/internal/store"
	"github.com/sorotrail/sorolens/internal/web"
)

// shutdownTimeout bounds how long in-flight requests may finish after a signal.
const shutdownTimeout = 15 * time.Second

func main() {
	if err := run(); err != nil {
		// The logger may not exist yet if configuration failed, so report the
		// startup failure on stderr unconditionally.
		fmt.Fprintf(os.Stderr, "sorolens: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	// Cancelled on SIGINT/SIGTERM, which unwinds the ingester and the server.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := metrics.New()
	var broker *api.Broker
	if cfg.SourceMode == config.ModeRPC {
		broker = api.NewBroker()
	}

	src, cleanup, startIngest, err := buildSource(ctx, cfg, log, m, broker)
	if err != nil {
		return err
	}
	defer cleanup()

	log.Info("starting sorolens",
		"mode", cfg.SourceMode,
		"http_addr", cfg.HTTPAddr)

	var wg sync.WaitGroup
	if startIngest != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := startIngest(ctx); err != nil {
				log.Error("ingester exited", "error", err)
			}
		}()
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router(src, log, m, cfg.CORSAllowedOrigins, broker),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		stop()
		broker.Shutdown()
		wg.Wait()
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	// Close active SSE streams before waiting on srv.Shutdown so long-lived
	// subscribers exit promptly within shutdownTimeout.
	broker.Shutdown()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}

	wg.Wait()
	log.Info("sorolens stopped")
	return nil
}

// buildSource wires the EventSource for the configured mode. It returns the
// source, a cleanup function, and — in standalone mode — the ingester's run
// function, which is nil in upstream mode.
//
// contributors: adding a new SOURCE_MODE means adding a case here and an
// implementation under internal/source. Nothing else in the program needs to
// know which backend is in use.
func buildSource(ctx context.Context, cfg config.Config, log *slog.Logger, m *metrics.Metrics, broker *api.Broker) (
	src source.EventSource,
	cleanup func(),
	startIngest func(context.Context) error,
	err error,
) {
	switch cfg.SourceMode {
	case config.ModeSoroTrail:
		log.Info("using upstream sorotrail source", "url", cfg.SoroTrailURL)
		return sorotrailsource.New(cfg.SoroTrailURL, nil), func() {}, nil, nil

	case config.ModeRPC:
		log.Info("using standalone rpc source", "rpc_url", cfg.RPCURL)

		if err := store.Migrate(cfg.DatabaseURL); err != nil {
			return nil, nil, nil, err
		}
		log.Info("migrations applied")

		pg, err := store.NewPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			return nil, nil, nil, err
		}

		client := rpc.NewHTTPClient(cfg.RPCURL, nil)

		// Verify the endpoint really is the configured network before
		// ingesting anything: a mainnet endpoint behind testnet config makes
		// the explorer quietly show the wrong chain's activity.
		if net, err := client.GetNetwork(ctx); err != nil {
			log.Warn("could not verify network passphrase", "error", err)
		} else if err := config.VerifyPassphrase(cfg.Network.Passphrase, net.Passphrase); err != nil {
			return nil, nil, nil, err
		}
		log.Info("network verified", "network", cfg.Network.Name, "rpc_url", cfg.RPCURL)

		client.OnXDRFallback(func() {
			log.Warn("stellar rpc rejected xdrFormat=json; decoding base64 XDR locally instead")
		})

		ingester := ingest.New(client, pg, decode.NewXDRDecoder(), ingest.Options{
			WatchedContracts: cfg.WatchedContracts,
			PollInterval:     cfg.PollInterval,
			StartLedger:      cfg.StartLedger,
			RetentionLedgers: cfg.RetentionLedgers,
		}, log)

		ingester.WithMetrics(m).WithPublisher(broker)

		return rpcsource.New(pg, client), pg.Close, ingester.Run, nil

	default:
		return nil, nil, nil, fmt.Errorf("unsupported SOURCE_MODE %q", cfg.SourceMode)
	}
}

// router mounts the web UI at the root and the JSON API under /api.
func router(src source.EventSource, log *slog.Logger, m *metrics.Metrics, corsOrigins []string, broker ...*api.Broker) http.Handler {
	apiServer := api.New(src, log)
	if len(broker) > 0 && broker[0] != nil {
		apiServer.WithBroker(broker[0])
	}

	webServer, err := web.New(src, log)
	if err != nil {
		// Templates are embedded, so a parse failure is a build-time mistake
		// that must surface loudly rather than degrade at runtime.
		panic(fmt.Sprintf("parsing web templates: %v", err))
	}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer, reqid.Middleware, requestLogger(log))
	r.Use(api.CORSMiddleware(api.CORSConfig{Origins: corsOrigins}))
	// RoutePattern returns the matched chi pattern (e.g. "/api/events/{id}"),
	// not the raw path, keeping metric label cardinality bounded.
	metrics.RoutePattern = func(req *http.Request) string {
		if rc := chi.RouteContext(req.Context()); rc != nil {
			return rc.RoutePattern()
		}
		return ""
	}
	r.Use(m.Middleware)

	// /metrics is unauthenticated like the rest of SoroLens's read surface:
	// it exposes route timings and ingest counters, no event data.
	r.Handle("/metrics", m.Handler())
	r.Get("/health", apiServer.HealthHandler())
	r.Get("/livez", apiServer.LivezHandler())
	r.Get("/readyz", apiServer.ReadyzHandler())
	r.Mount("/api", apiServer.Routes())
	r.Mount("/", webServer.Routes())

	return r
}

func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)
			log.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration", time.Since(start),
				"remote", r.RemoteAddr,
				"request_id", reqid.From(r),
			)
		})
	}
}
