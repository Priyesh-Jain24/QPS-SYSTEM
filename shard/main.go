package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/blevesearch/bleve/v2"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SearchResult is a single ranked match returned by this shard.
type SearchResult struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Score     float64   `json:"score"`
	Embedding []float64 `json:"embedding,omitempty"`
}

// SearchResponse is the full payload returned from /search.
type SearchResponse struct {
	ShardID string         `json:"shard_id"`
	Query   string         `json:"query"`
	Results []SearchResult `json:"results"`
	TookMs  int64          `json:"took_ms"`
	NumDocs int            `json:"num_docs_in_shard"`
}

var (
	index         bleve.Index
	shardID       string
	docEmbeddings sync.Map // ID -> []float64 mapping for faster indexing integration
)

func buildIndex(docs []Document) (bleve.Index, error) {
	mapping := bleve.NewIndexMapping()
	idx, err := bleve.NewMemOnly(mapping)
	if err != nil {
		return nil, err
	}
	for _, d := range docs {
		if err := idx.Index(d.ID, d); err != nil {
			return nil, err
		}
	}
	return idx, nil
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "missing required query param 'q'", http.StatusBadRequest)
		return
	}

	query := bleve.NewMatchQuery(q)
	searchReq := bleve.NewSearchRequest(query)
	searchReq.Size = 10
	searchReq.Fields = []string{"title"} // pull stored Title field back with each hit

	res, err := index.Search(searchReq)
	if err != nil {
		http.Error(w, "search failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	results := make([]SearchResult, 0, len(res.Hits))
	for _, hit := range res.Hits {
		title := ""
		if t, ok := hit.Fields["title"].(string); ok {
			title = t
		}
		var emb []float64
		if val, ok := docEmbeddings.Load(hit.ID); ok {
			emb = val.([]float64)
		}
		results = append(results, SearchResult{
			ID:        hit.ID,
			Title:     title,
			Score:     hit.Score,
			Embedding: emb,
		})
	}

	docCount, _ := index.DocCount()

	resp := SearchResponse{
		ShardID: shardID,
		Query:   q,
		Results: results,
		TookMs:  time.Since(start).Milliseconds(),
		NumDocs: int(docCount),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	shardStartTime = time.Now()
	shardID = getenv("SHARD_ID", "shard1")
	ollamaURL = getenv("OLLAMA_URL", "")
	port := getenv("PORT", "8081")
	shardAddr := getenv("SHARD_ADDR", "localhost:"+port) // must be reachable from coordinator, e.g. "shard1:8081"
	etcdEndpoints := getenv("ETCD_ENDPOINTS", "etcd:2379")

	docs := seedDocuments(shardID)
	// Compute embeddings for seed documents on startup in the background.
	// This ensures startup doesn't hang if Ollama is still pulling the model.
	go func() {
		for _, d := range docs {
			if emb, err := getEmbedding(d.Title + " " + d.Content); err == nil && len(emb) > 0 {
				docEmbeddings.Store(d.ID, emb)
			}
		}
	}()

	idx, err := buildIndex(docs)
	if err != nil {
		log.Fatalf("[%s] failed to build index: %v", shardID, err)
	}
	index = idx

	http.HandleFunc("/search", searchHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/index", ingestHandler)
	http.HandleFunc("/stats", statsHandler)

	srv := &http.Server{Addr: ":" + port}

	go func() {
		log.Printf("[%s] shard listening on :%s with %d documents indexed", shardID, port, len(docs))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] server failed: %v", shardID, err)
		}
	}()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("[%s] failed to connect to etcd: %v", shardID, err)
	}
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaseID, err := RegisterShard(ctx, cli, ShardInfo{ID: shardID, Addr: shardAddr}, 10)
	if err != nil {
		log.Fatalf("[%s] failed to register with etcd: %v", shardID, err)
	}

	// Block until SIGINT/SIGTERM, then deregister immediately rather than
	// waiting for the etcd lease to expire.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Printf("[%s] shutting down, revoking etcd lease", shardID)
	cancel()
	cli.Revoke(context.Background(), leaseID)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)
}
