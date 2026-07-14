package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "sort"
    "strings"
    "sync"
    "time"

    "github.com/redis/go-redis/v9"
    clientv3 "go.etcd.io/etcd/client/v3"
)

// ShardResult mirrors the response shape returned by a shard's /search endpoint.
type ShardResult struct {
	ShardID string `json:"shard_id"`
	Query   string `json:"query"`
	Results []struct {
		ID    string  `json:"id"`
		Title string  `json:"title"`
		Score float64 `json:"score"`
	} `json:"results"`
	TookMs int64 `json:"took_ms"`
}

// MergedResult is a single ranked hit in the coordinator's final response.
type MergedResult struct {
	ID      string  `json:"id"`
	Title   string  `json:"title"`
	Score   float64 `json:"score"`
	ShardID string  `json:"shard_id"`
}

// CoordinatorResponse is the final payload returned to the client.
type CoordinatorResponse struct {
	Query      string         `json:"query"`
	Cached     bool           `json:"cached"`
	Results    []MergedResult `json:"results"`
	ShardsUsed []string       `json:"shards_used"`
	TookMs     int64          `json:"took_ms"`
}

var (
	shardRegistry *ShardRegistry
	rdb           *redis.Client
	cacheTTL      = 60 * time.Second
	shardTimeout  = 2 * time.Second
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/search?q="+query, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
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
		return nil, nil
	}

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		merged     []MergedResult
		shardsUsed []string
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
					ID:      r.ID,
					Title:   r.Title,
					Score:   r.Score,
					ShardID: result.ShardID,
				})
			}
		}(s)
	}

	wg.Wait()

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score > merged[j].Score
	})

	return merged, shardsUsed
}

func cacheKey(query string) string {
	return "searchsphere:query:" + strings.ToLower(strings.TrimSpace(query))
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "missing required query param 'q'", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	key := cacheKey(query)

	// Try cache first.
	if rdb != nil {
		if cached, err := rdb.Get(ctx, key).Result(); err == nil {
			var resp CoordinatorResponse
			if json.Unmarshal([]byte(cached), &resp) == nil {
				resp.Cached = true
				resp.TookMs = time.Since(start).Milliseconds()
				writeJSON(w, resp)
				return
			}
		}
	}

	merged, shardsUsed := fanOut(query)

	resp := CoordinatorResponse{
		Query:      query,
		Cached:     false,
		Results:    merged,
		ShardsUsed: shardsUsed,
		TookMs:     time.Since(start).Milliseconds(),
	}

	// Best-effort cache write; failures here shouldn't break the response.
	if rdb != nil {
		if data, err := json.Marshal(resp); err == nil {
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
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func main() {
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

	// ...redis setup unchanged...

	port := getenv("PORT", "8080")

	http.HandleFunc("/search", searchHandler)
	http.HandleFunc("/health", healthHandler)

	log.Printf("coordinator listening on :%s (shard list is dynamic via etcd)", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}