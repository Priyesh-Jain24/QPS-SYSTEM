package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Request Logging Middleware
// ---------------------------------------------------------------------------

// statusWriter wraps http.ResponseWriter to capture the written status code.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.code = code
	sw.ResponseWriter.WriteHeader(code)
}

// loggingMiddleware logs every request with method, path, status, latency, and
// client IP in a structured, single-line format.
func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}

		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			b := make([]byte, 8)
			rand.Read(b)
			reqID = hex.EncodeToString(b)
		}

		ctx := context.WithValue(r.Context(), "request_id", reqID)
		r = r.WithContext(ctx)

		clientIP := r.RemoteAddr
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			clientIP = strings.Split(xff, ",")[0]
		}

		next(sw, r)

		log.Printf(`{"level":"info","request_id":"%s","method":"%s","path":"%s","status":%d,"latency_ms":%d,"client_ip":"%s"}`,
			reqID, r.Method, r.URL.Path, sw.code,
			time.Since(start).Milliseconds(), clientIP,
		)
	}
}

// ---------------------------------------------------------------------------
// Token-Bucket Rate Limiter
// ---------------------------------------------------------------------------

// RateLimiter implements a token-bucket rate limiter.
type RateLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// NewRateLimiter creates a rate limiter with the given max requests per second.
func NewRateLimiter(rps float64) *RateLimiter {
	return &RateLimiter{
		tokens:     rps,
		maxTokens:  rps,
		refillRate: rps,
		lastRefill: time.Now(),
	}
}

// Allow returns true if the request is allowed (a token is available).
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	rl.tokens += elapsed * rl.refillRate
	if rl.tokens > rl.maxTokens {
		rl.tokens = rl.maxTokens
	}
	rl.lastRefill = now

	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}

var globalLimiter *RateLimiter

func initRateLimiter() {
	rps := 100.0 // default: 100 requests per second
	if v := os.Getenv("RATE_LIMIT"); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil && parsed > 0 {
			rps = parsed
		}
	}
	globalLimiter = NewRateLimiter(rps)
	log.Printf("rate limiter initialized: %.0f req/s", rps)
}

// rateLimitMiddleware rejects requests that exceed the configured rate.
func rateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if globalLimiter != nil && !globalLimiter.Allow() {
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

// ---------------------------------------------------------------------------
// Prometheus-Compatible Metrics (zero-dependency)
// ---------------------------------------------------------------------------

var (
	metricsTotalRequests  int64
	metricsTotal2xx       int64
	metricsTotal4xx       int64
	metricsTotal5xx       int64
	metricsTotalLatencyMs int64 // cumulative latency in ms for average calc
	metricsActiveRequests int64
	metricsSearchRequests int64
	metricsIndexRequests  int64
	metricsDeleteRequests int64
	metricsBulkRequests   int64
)

// metricsMiddleware increments Prometheus-like counters and tracks latency.
func metricsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&metricsActiveRequests, 1)
		defer atomic.AddInt64(&metricsActiveRequests, -1)

		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}

		// Track per-endpoint counters.
		path := r.URL.Path
		switch {
		case path == "/search":
			atomic.AddInt64(&metricsSearchRequests, 1)
		case path == "/index":
			atomic.AddInt64(&metricsIndexRequests, 1)
		case path == "/bulk":
			atomic.AddInt64(&metricsBulkRequests, 1)
		case strings.HasPrefix(path, "/documents/") && r.Method == http.MethodDelete:
			atomic.AddInt64(&metricsDeleteRequests, 1)
		}

		next(sw, r)

		latency := time.Since(start).Milliseconds()
		atomic.AddInt64(&metricsTotalRequests, 1)
		atomic.AddInt64(&metricsTotalLatencyMs, latency)

		switch {
		case sw.code >= 200 && sw.code < 300:
			atomic.AddInt64(&metricsTotal2xx, 1)
		case sw.code >= 400 && sw.code < 500:
			atomic.AddInt64(&metricsTotal4xx, 1)
		case sw.code >= 500:
			atomic.AddInt64(&metricsTotal5xx, 1)
		}
	}
}

// metricsHandler returns Prometheus text exposition format.
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	total := atomic.LoadInt64(&metricsTotalRequests)
	totalLatency := atomic.LoadInt64(&metricsTotalLatencyMs)
	avgLatency := 0.0
	if total > 0 {
		avgLatency = float64(totalLatency) / float64(total)
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# HELP searchsphere_requests_total Total HTTP requests.\n")
	fmt.Fprintf(w, "# TYPE searchsphere_requests_total counter\n")
	fmt.Fprintf(w, "searchsphere_requests_total %d\n\n", total)

	fmt.Fprintf(w, "# HELP searchsphere_requests_by_status HTTP requests by status class.\n")
	fmt.Fprintf(w, "# TYPE searchsphere_requests_by_status counter\n")
	fmt.Fprintf(w, "searchsphere_requests_by_status{class=\"2xx\"} %d\n", atomic.LoadInt64(&metricsTotal2xx))
	fmt.Fprintf(w, "searchsphere_requests_by_status{class=\"4xx\"} %d\n", atomic.LoadInt64(&metricsTotal4xx))
	fmt.Fprintf(w, "searchsphere_requests_by_status{class=\"5xx\"} %d\n\n", atomic.LoadInt64(&metricsTotal5xx))

	fmt.Fprintf(w, "# HELP searchsphere_requests_by_endpoint Requests per endpoint.\n")
	fmt.Fprintf(w, "# TYPE searchsphere_requests_by_endpoint counter\n")
	fmt.Fprintf(w, "searchsphere_requests_by_endpoint{endpoint=\"search\"} %d\n", atomic.LoadInt64(&metricsSearchRequests))
	fmt.Fprintf(w, "searchsphere_requests_by_endpoint{endpoint=\"index\"} %d\n", atomic.LoadInt64(&metricsIndexRequests))
	fmt.Fprintf(w, "searchsphere_requests_by_endpoint{endpoint=\"delete\"} %d\n", atomic.LoadInt64(&metricsDeleteRequests))
	fmt.Fprintf(w, "searchsphere_requests_by_endpoint{endpoint=\"bulk\"} %d\n\n", atomic.LoadInt64(&metricsBulkRequests))

	fmt.Fprintf(w, "# HELP searchsphere_active_requests Currently in-flight requests.\n")
	fmt.Fprintf(w, "# TYPE searchsphere_active_requests gauge\n")
	fmt.Fprintf(w, "searchsphere_active_requests %d\n\n", atomic.LoadInt64(&metricsActiveRequests))

	fmt.Fprintf(w, "# HELP searchsphere_avg_latency_ms Average request latency in milliseconds.\n")
	fmt.Fprintf(w, "# TYPE searchsphere_avg_latency_ms gauge\n")
	fmt.Fprintf(w, "searchsphere_avg_latency_ms %.2f\n\n", avgLatency)

	fmt.Fprintf(w, "# HELP searchsphere_cache_hits Total cache hits.\n")
	fmt.Fprintf(w, "# TYPE searchsphere_cache_hits counter\n")
	fmt.Fprintf(w, "searchsphere_cache_hits %d\n\n", atomic.LoadInt64(&cacheHits))

	fmt.Fprintf(w, "# HELP searchsphere_cache_misses Total cache misses.\n")
	fmt.Fprintf(w, "# TYPE searchsphere_cache_misses counter\n")
	fmt.Fprintf(w, "searchsphere_cache_misses %d\n\n", atomic.LoadInt64(&cacheMisses))

	fmt.Fprintf(w, "# HELP searchsphere_uptime_seconds Coordinator uptime.\n")
	fmt.Fprintf(w, "# TYPE searchsphere_uptime_seconds gauge\n")
	fmt.Fprintf(w, "searchsphere_uptime_seconds %.0f\n", time.Since(startTime).Seconds())
}

// ---------------------------------------------------------------------------
// Authentication Middleware
// ---------------------------------------------------------------------------

var adminAPIKey string

func initAuth() {
	adminAPIKey = os.Getenv("API_KEY")
	if adminAPIKey != "" {
		log.Printf("admin API key authentication enabled")
	} else {
		log.Printf("WARNING: API_KEY not set - mutation endpoints are open")
	}
}

// adminAuthMiddleware requires a Bearer token matching API_KEY, if set.
func adminAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if adminAPIKey == "" {
			next(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token != adminAPIKey {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}

		next(w, r)
	}
}

// wrapMiddleware chains: logging → rate limit → metrics → cors → handler
func wrapMiddleware(handler http.HandlerFunc) http.HandlerFunc {
	return loggingMiddleware(rateLimitMiddleware(metricsMiddleware(corsMiddleware(handler))))
}

// wrapAdminMiddleware adds admin Auth to the chain.
func wrapAdminMiddleware(handler http.HandlerFunc) http.HandlerFunc {
	return loggingMiddleware(rateLimitMiddleware(metricsMiddleware(corsMiddleware(adminAuthMiddleware(handler)))))
}
