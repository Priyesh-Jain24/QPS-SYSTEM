package main

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

// CircuitState represents the current state of a circuit breaker.
type CircuitState int

const (
	CircuitClosed   CircuitState = iota // healthy, requests flow through
	CircuitOpen                         // tripped, requests are rejected
	CircuitHalfOpen                     // testing, one request allowed through
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ShardCircuit tracks the health of a single shard connection.
type ShardCircuit struct {
	State       CircuitState
	Failures    int
	LastFailure time.Time
	LastAttempt time.Time
}

// CircuitBreaker manages per-shard circuit breakers.
type CircuitBreaker struct {
	mu        sync.RWMutex
	circuits  map[string]*ShardCircuit // keyed by shard address
	threshold int                      // failures before tripping
	cooldown  time.Duration            // time before moving from open → half-open
}

// NewCircuitBreaker creates a circuit breaker with config from env vars.
func NewCircuitBreaker() *CircuitBreaker {
	threshold := 3
	if v := os.Getenv("CB_THRESHOLD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			threshold = n
		}
	}

	cooldown := 10 * time.Second
	if v := os.Getenv("CB_COOLDOWN"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cooldown = d
		}
	}

	log.Printf("circuit breaker initialized: threshold=%d, cooldown=%v", threshold, cooldown)
	return &CircuitBreaker{
		circuits:  make(map[string]*ShardCircuit),
		threshold: threshold,
		cooldown:  cooldown,
	}
}

// AllowRequest checks if a request to the given shard should be allowed.
// Returns true if the circuit is closed or half-open (test request).
func (cb *CircuitBreaker) AllowRequest(addr string) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	sc, exists := cb.circuits[addr]
	if !exists {
		// No circuit yet → closed by default.
		cb.circuits[addr] = &ShardCircuit{State: CircuitClosed}
		return true
	}

	switch sc.State {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if cooldown has elapsed → move to half-open.
		if time.Since(sc.LastFailure) > cb.cooldown {
			sc.State = CircuitHalfOpen
			sc.LastAttempt = time.Now()
			log.Printf("circuit breaker [%s]: open → half-open (cooldown elapsed)", addr)
			return true
		}
		return false
	case CircuitHalfOpen:
		// Only allow one concurrent probe.
		return false
	}
	return true
}

// RecordSuccess resets the circuit for a shard back to closed.
func (cb *CircuitBreaker) RecordSuccess(addr string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	sc, exists := cb.circuits[addr]
	if !exists {
		return
	}

	if sc.State != CircuitClosed {
		log.Printf("circuit breaker [%s]: %s → closed (success)", addr, sc.State)
	}
	sc.State = CircuitClosed
	sc.Failures = 0
}

// RecordFailure increments the failure count and may trip the circuit open.
func (cb *CircuitBreaker) RecordFailure(addr string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	sc, exists := cb.circuits[addr]
	if !exists {
		sc = &ShardCircuit{State: CircuitClosed}
		cb.circuits[addr] = sc
	}

	sc.Failures++
	sc.LastFailure = time.Now()

	if sc.Failures >= cb.threshold {
		if sc.State != CircuitOpen {
			log.Printf("circuit breaker [%s]: %s → open (failures=%d >= threshold=%d)",
				addr, sc.State, sc.Failures, cb.threshold)
		}
		sc.State = CircuitOpen
	}
}

// Status returns a snapshot of all circuit states (for health/metrics).
func (cb *CircuitBreaker) Status() map[string]string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	out := make(map[string]string, len(cb.circuits))
	for addr, sc := range cb.circuits {
		out[addr] = sc.State.String()
	}
	return out
}
