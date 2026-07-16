package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ShardResult mirrors the response shape returned by a shard's /search endpoint.
type ShardResult struct {
	ShardID string `json:"shard_id"`
	Query   string `json:"query"`
	Results []struct {
		ID        string    `json:"id"`
		Title     string    `json:"title"`
		Score     float64   `json:"score"`
		Embedding []float64 `json:"embedding,omitempty"`
	} `json:"results"`
	TookMs int64 `json:"took_ms"`
}

// MergedResult is a single ranked hit in the coordinator's final response.
type MergedResult struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Score     float64   `json:"score"`
	ShardID   string    `json:"shard_id"`
	Embedding []float64 `json:"-"` // Used internally by reranker, not returned to client
}

// CoordinatorResponse is the final payload returned to the client.
type CoordinatorResponse struct {
	Query        string         `json:"query"`
	Cached       bool           `json:"cached"`
	Results      []MergedResult `json:"results"`
	TotalResults int            `json:"total_results"`
	Page         int            `json:"page"`
	PageSize     int            `json:"page_size"`
	ShardsUsed   []string       `json:"shards_used"`
	TookMs       int64          `json:"took_ms"`
}

var (
	shardRegistry *ShardRegistry
	rdb           *redis.Client
	cacheTTL      = 60 * time.Second
	shardTimeout  = 2 * time.Second
	startTime     time.Time

	// Dedicated HTTP client for shard communication with tight timeouts.
	shardClient = &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     30 * time.Second,
			DialContext: (&net.Dialer{
				Timeout: 1 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: 2 * time.Second,
		},
	}

	// Atomic counters for cache observability.
	cacheHits   int64
	cacheMisses int64

	// Consistent hash ring for document routing.
	hashRing *HashRing
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// queryShard hits a single shard's /search endpoint and returns its results.
// Errors and timeouts are returned to the caller rather than swallowed, so
// the fan-out logic can decide whether to treat a missing shard as fatal
// or return partial results.
func queryShard(ctx context.Context, addr, query string) (*ShardResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/search?q="+url.QueryEscape(query), nil)
	if err != nil {
		return nil, err
	}

	resp, err := shardClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result ShardResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// fanOut queries all known shards concurrently and returns whatever
// results come back within the timeout. Slow or dead shards are skipped
// rather than failing the whole query (partial results > no results).
func fanOut(query string) ([]MergedResult, []string) {
	ctx, cancel := context.WithTimeout(context.Background(), shardTimeout)
	defer cancel()

	shards := shardRegistry.List()
	if len(shards) == 0 {
		log.Printf("no shards currently registered in etcd")
		return []MergedResult{}, []string{}
	}

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		merged     = make([]MergedResult, 0)
		shardsUsed = make([]string, 0)
	)

	for _, s := range shards {
		wg.Add(1)
		go func(s ShardInfo) {
			defer wg.Done()

			result, err := queryShard(ctx, "http://"+s.Addr, query)
			if err != nil {
				log.Printf("shard %s (%s) failed or timed out: %v", s.ID, s.Addr, err)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			shardsUsed = append(shardsUsed, result.ShardID)
			for _, r := range result.Results {
				merged = append(merged, MergedResult{
					ID:        r.ID,
					Title:     r.Title,
					Score:     r.Score,
					ShardID:   result.ShardID,
					Embedding: r.Embedding,
				})
			}
		}(s)
	}

	wg.Wait()

	// Sort by score descending.
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score > merged[j].Score
	})

	// Deduplicate by document ID, keeping the highest-scoring entry.
	seen := make(map[string]struct{}, len(merged))
	deduped := make([]MergedResult, 0, len(merged))
	for _, r := range merged {
		if _, exists := seen[r.ID]; !exists {
			seen[r.ID] = struct{}{}
			deduped = append(deduped, r)
		}
	}

	return deduped, shardsUsed
}

// cacheKey normalises the query so that "redis go" and "Go Redis" resolve
// to the same cache entry: lowercase, trim, split on whitespace, sort, rejoin.
func cacheKey(query string) string {
	words := strings.Fields(strings.ToLower(strings.TrimSpace(query)))
	sort.Strings(words)
	return "searchsphere:query:" + strings.Join(words, " ")
}

// parsePagination extracts page and page_size from query params with defaults.
func parsePagination(r *http.Request) (page, pageSize int) {
	page = 1
	pageSize = 10
	if v := r.URL.Query().Get("page"); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 {
			page = p
		}
	}
	if v := r.URL.Query().Get("page_size"); v != "" {
		if ps, err := strconv.Atoi(v); err == nil && ps > 0 {
			pageSize = ps
		}
	}
	if pageSize > 100 {
		pageSize = 100
	}
	return
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "missing required query param 'q'", http.StatusBadRequest)
		return
	}

	page, pageSize := parsePagination(r)
	ctx := r.Context()
	key := cacheKey(query)

	// Try cache first.
	if rdb != nil {
		if cached, err := rdb.Get(ctx, key).Result(); err == nil {
			var resp CoordinatorResponse
			if json.Unmarshal([]byte(cached), &resp) == nil {
				hits := atomic.AddInt64(&cacheHits, 1)
				log.Printf("cache HIT for %q (total hits: %d)", query, hits)
				resp.Cached = true
				resp.TookMs = time.Since(start).Milliseconds()
				// Re-paginate cached results.
				all := resp.Results
				resp.TotalResults = len(all)
				resp.Page = page
				resp.PageSize = pageSize
				start := (page - 1) * pageSize
				if start >= len(all) {
					resp.Results = []MergedResult{}
				} else {
					end := start + pageSize
					if end > len(all) {
						end = len(all)
					}
					resp.Results = all[start:end]
				}
				writeJSON(w, resp)
				return
			}
		}
	}

	if rdb != nil {
		misses := atomic.AddInt64(&cacheMisses, 1)
		log.Printf("cache MISS for %q (total misses: %d)", query, misses)
	}

	merged, shardsUsed := fanOut(query)
	merged = Rerank(query, merged)
	totalResults := len(merged)

	// Paginate results.
	pageStart := (page - 1) * pageSize
	pagedResults := []MergedResult{}
	if pageStart < totalResults {
		pageEnd := pageStart + pageSize
		if pageEnd > totalResults {
			pageEnd = totalResults
		}
		pagedResults = merged[pageStart:pageEnd]
	}

	resp := CoordinatorResponse{
		Query:        query,
		Cached:       false,
		Results:      pagedResults,
		TotalResults: totalResults,
		Page:         page,
		PageSize:     pageSize,
		ShardsUsed:   shardsUsed,
		TookMs:       time.Since(start).Milliseconds(),
	}

	// Cache the full (unpaginated) result set so any page can be served from cache.
	if rdb != nil {
		cacheResp := resp
		cacheResp.Results = merged
		if data, err := json.Marshal(cacheResp); err == nil {
			if err := rdb.Set(ctx, key, data, cacheTTL).Err(); err != nil {
				log.Printf("redis cache write failed: %v", err)
			}
		}
	}

	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	redisOK := false
	if rdb != nil {
		if err := rdb.Ping(r.Context()).Err(); err == nil {
			redisOK = true
		}
	}

	ollamaOK := false
	if ollamaURL != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ollamaURL, nil)
		if resp, err := shardClient.Do(req); err == nil {
			resp.Body.Close()
			ollamaOK = true
		}
	}

	status := "healthy"
	code := http.StatusOK
	shardCount := shardRegistry.Count()
	if shardCount == 0 {
		status = "degraded"
		code = http.StatusOK // still respond 200 but flag as degraded
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":           status,
		"shard_count":      shardCount,
		"redis_connected":  redisOK,
		"ollama_connected": ollamaOK,
		"uptime_seconds":   int64(time.Since(startTime).Seconds()),
		"cache_hits":       atomic.LoadInt64(&cacheHits),
		"cache_misses":     atomic.LoadInt64(&cacheMisses),
	})
}

// cacheHandler supports:
//
//	DELETE /cache         → flush the entire search cache
//	DELETE /cache?q=...   → invalidate a single query's cache entry
//	GET    /cache         → return cache hit/miss stats
func cacheHandler(w http.ResponseWriter, r *http.Request) {
	if rdb == nil {
		http.Error(w, "caching is disabled (no Redis connection)", http.StatusServiceUnavailable)
		return
	}

	ctx := r.Context()

	switch r.Method {
	case http.MethodDelete:
		q := r.URL.Query().Get("q")
		if q != "" {
			// Invalidate a single query.
			key := cacheKey(q)
			del, err := rdb.Del(ctx, key).Result()
			if err != nil {
				http.Error(w, "cache delete failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			writeJSON(w, map[string]interface{}{"deleted_key": key, "keys_removed": del})
			log.Printf("cache invalidated for query %q (key: %s)", q, key)
			return
		}

		// Flush all search cache keys.
		var cursor uint64
		var totalDeleted int64
		for {
			keys, nextCursor, err := rdb.Scan(ctx, cursor, "searchsphere:query:*", 100).Result()
			if err != nil {
				http.Error(w, "cache flush failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if len(keys) > 0 {
				del, _ := rdb.Del(ctx, keys...).Result()
				totalDeleted += del
			}
			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
		writeJSON(w, map[string]interface{}{"flushed": true, "keys_removed": totalDeleted})
		log.Printf("cache flushed: %d keys removed", totalDeleted)

	case http.MethodGet:
		writeJSON(w, map[string]interface{}{
			"cache_hits":   atomic.LoadInt64(&cacheHits),
			"cache_misses": atomic.LoadInt64(&cacheMisses),
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// indexHandler accepts a document and routes it to the correct shard
// using consistent hashing on the document ID.
//
//	POST /index
//	Body: {"id": "...", "title": "...", "content": "..."}
func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var doc struct {
		ID      string `json:"id"`
		Title   string `json:"title"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if doc.ID == "" {
		http.Error(w, "document 'id' is required", http.StatusBadRequest)
		return
	}

	// Rebuild hash ring from current shards.
	hashRing.Update(shardRegistry.List())

	targetAddr := hashRing.GetShard(doc.ID)
	if targetAddr == "" {
		http.Error(w, "no shards available for indexing", http.StatusServiceUnavailable)
		return
	}

	// Forward to the target shard.
	shardURL := "http://" + targetAddr + "/index"
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, shardURL, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "failed to create shard request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := shardClient.Do(req)
	if err != nil {
		http.Error(w, "shard indexing request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Stream the shard's response back to the client.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)

	log.Printf("document %q routed to shard %s", doc.ID, targetAddr)
}

// corsMiddleware wraps a handler to add permissive CORS headers so
// browser-based clients can call the API directly.
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func main() {
	startTime = time.Now()
	hashRing = NewHashRing(150) // 150 virtual nodes per shard for even distribution
	ollamaURL = getenv("OLLAMA_URL", "")
	etcdEndpoints := getenv("ETCD_ENDPOINTS", "etcd:2379")

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	shardRegistry = NewShardRegistry()
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	if err := shardRegistry.Watch(watchCtx, cli); err != nil {
		log.Fatalf("failed to start shard watch: %v", err)
	}

	redisAddr := getenv("REDIS_ADDR", "")
	if redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			log.Printf("redis not available, caching disabled: %v", err)
			rdb = nil
		} else {
			log.Printf("redis connected at %s", redisAddr)
		}
	}

	port := getenv("PORT", "8080")

	http.HandleFunc("/search", corsMiddleware(searchHandler))
	http.HandleFunc("/health", corsMiddleware(healthHandler))
	http.HandleFunc("/cache", corsMiddleware(cacheHandler))
	http.HandleFunc("/index", corsMiddleware(indexHandler))

	srv := &http.Server{Addr: ":" + port}

	// Start serving in background.
	go func() {
		log.Printf("coordinator listening on :%s (shard list is dynamic via etcd)", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	}()

	// Block until SIGINT/SIGTERM for graceful shutdown.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("coordinator shutting down gracefully...")
	watchCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	if rdb != nil {
		rdb.Close()
	}
	cli.Close()
	log.Println("coordinator stopped")
}
