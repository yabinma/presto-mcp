package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
)

// shutdownGrace bounds how long an in-flight request has to drain on shutdown.
const shutdownGrace = 10 * time.Second

// runHTTP serves the MCP server over the streamable-HTTP transport (enterprise
// shape) with edge hardening: optional bearer verification, Origin allow-listing,
// request timeouts, a /healthz probe, optional TLS, and graceful shutdown driven
// by ctx (the signal context from main).
func runHTTP(ctx context.Context, hc *config.HTTPConfig, s *mcp.Server) error {
	if hc == nil {
		return fmt.Errorf("http transport requires server.http configuration")
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(hc.Host, strconv.Itoa(hc.Port)))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	return serveHTTP(ctx, ln, hc, s)
}

// serveHTTP runs the hardened MCP HTTP server on ln until ctx is cancelled, then
// drains gracefully. It is split from runHTTP so tests can supply an ephemeral
// listener and learn its address.
func serveHTTP(ctx context.Context, ln net.Listener, hc *config.HTTPConfig, s *mcp.Server) error {
	// Logs go to stderr: in stdio mode stdout carries the MCP protocol, so the
	// codebase never logs there; stderr is safe in both shapes.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	handler, err := newHTTPHandler(hc, s, logger)
	if err != nil {
		return err
	}
	tlsConf, err := tlsConfig(hc)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		TLSConfig:         tlsConf,
		// No WriteTimeout: streamable HTTP holds long-lived SSE response streams.
	}

	scheme := "http"
	if tlsConf != nil {
		scheme = "https"
	}
	logger.Info("presto-mcp serving",
		"transport", "http", "addr", ln.Addr().String(), "scheme", scheme, "edge_auth", hc.EdgeAuth != nil)

	serveErr := make(chan error, 1)
	go func() {
		if tlsConf != nil {
			serveErr <- srv.ServeTLS(ln, "", "")
		} else {
			serveErr <- srv.Serve(ln)
		}
	}()

	select {
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		return srv.Shutdown(sctx)
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// newHTTPHandler assembles the MCP streamable handler with the /healthz probe,
// optional edge bearer verification, and the Origin guard.
func newHTTPHandler(hc *config.HTTPConfig, s *mcp.Server, logger *slog.Logger) (http.Handler, error) {
	// Stateful (the default): in verify mode the SDK pins a session to the
	// verified user (one credential per session); the caller's credential rides
	// each request regardless of session statefulness.
	mcpHandler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return s },
		&mcp.StreamableHTTPOptions{Logger: logger},
	)

	var protected http.Handler = mcpHandler
	if hc.EdgeAuth != nil {
		verifier, err := edgeVerifier(hc.EdgeAuth)
		if err != nil {
			return nil, fmt.Errorf("edge auth: %w", err)
		}
		// Verify the caller's bearer token before forwarding; on success the
		// TokenInfo (incl. the verified subject) rides the request context.
		protected = auth.RequireBearerToken(verifier, nil)(protected)
	}

	mux := http.NewServeMux()
	// Liveness/readiness probe — intentionally outside the bearer-auth wrapper.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/", protected)

	return originGuard(hc.AllowedOrigins, mux), nil
}

// originGuard rejects requests whose Origin header is not allow-listed (browser
// DNS-rebinding protection). An empty allowlist disables the check; a missing
// Origin (non-browser clients and probes send none) is always allowed.
func originGuard(allowed []string, next http.Handler) http.Handler {
	if len(allowed) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" || slices.Contains(allowed, origin) {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, "forbidden origin", http.StatusForbidden)
	})
}

// tlsConfig builds a TLS config from the cert/key references, or returns nil
// (plaintext) when no certificate is configured.
func tlsConfig(hc *config.HTTPConfig) (*tls.Config, error) {
	if hc.TLSCertRef == "" {
		return nil, nil
	}
	certPEM, err := credential.DefaultResolver(hc.TLSCertRef)
	if err != nil {
		return nil, fmt.Errorf("tls_cert_ref: %w", err)
	}
	keyPEM, err := credential.DefaultResolver(hc.TLSKeyRef)
	if err != nil {
		return nil, fmt.Errorf("tls_key_ref: %w", err)
	}
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		return nil, fmt.Errorf("tls keypair: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}
