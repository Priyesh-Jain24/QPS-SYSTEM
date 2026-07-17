package main

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/highlight/highlighter/ansi"
	"github.com/blevesearch/bleve/v2/search/query"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SearchResult is a single ranked match returned by this shard.
type SearchResult struct {
	ID         string            `json:"id"`
	Title      string            `json:"title"`
	Content    string            `json:"content,omitempty"`
	Score      float64           `json:"score"`
	Embedding  []float64         `json:"embedding,omitempty"`
	Highlights map[string]string `json:"highlights,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
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

// saveEmbeddings serializes the docEmbeddings sync.Map to a gob file.
func saveEmbeddings(path string) error {
	m := make(map[string][]float64)
	docEmbeddings.Range(func(k, v interface{}) bool {
		m[k.(string)] = v.([]float64)
		return true
	})

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return gob.NewEncoder(f).Encode(m)
}

// loadEmbeddings deserializes a gob file into docEmbeddings.
func loadEmbeddings(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // missing file is fine on first startup
		}
		return err
	}
	defer f.Close()

	var m map[string][]float64
	if err := gob.NewDecoder(f).Decode(&m); err != nil {
		return err
	}
	for k, v := range m {
		docEmbeddings.Store(k, v)
	}
	return nil
}

// buildIndex opens the index at dataDir if it exists, or creates it and seeds it.
func buildIndex(dataDir string, docs []Document) (bleve.Index, error) {
	indexPath := filepath.Join(dataDir, "index.bleve")
	idx, err := bleve.Open(indexPath)
	if err == nil {
		log.Printf("[%s] opened existing index at %s", shardID, indexPath)
		return idx, nil
	}
	if err != bleve.ErrorIndexPathDoesNotExist {
		return nil, fmt.Errorf("error opening index: %v", err)
	}

	log.Printf("[%s] creating new index at %s", shardID, indexPath)
	mapping := bleve.NewIndexMapping()
	idx, err = bleve.New(indexPath, mapping)
	if err != nil {
		return nil, err
	}

	// Seed documents and index their embeddings in background since this is a new index.
	for _, d := range docs {
		if err := idx.Index(d.ID, d); err != nil {
			return nil, err
		}
	}
	go func() {
		for _, d := range docs {
			if emb, err := getEmbedding(d.Title + " " + d.Content); err == nil && len(emb) > 0 {
				docEmbeddings.Store(d.ID, emb)
			}
		}
	}()

	return idx, nil
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "missing required query param 'q'", http.StatusBadRequest)
		return
	}

	// Build the query — select mode and optionally scope to a field.
	field := r.URL.Query().Get("field")
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "match"
	}

	var searchQuery query.Query
	switch mode {
	case "phrase":
		pq := bleve.NewMatchPhraseQuery(q)
		if field != "" {
			pq.SetField(field)
		}
		searchQuery = pq
	case "prefix":
		pq := bleve.NewPrefixQuery(q)
		if field != "" {
			pq.SetField(field)
		}
		searchQuery = pq
	case "fuzzy":
		fq := bleve.NewFuzzyQuery(q)
		fq.Fuzziness = 2
		if field != "" {
			fq.SetField(field)
		}
		searchQuery = fq
	default: // "match"
		mq := bleve.NewMatchQuery(q)
		if field != "" {
			mq.SetField(field)
		}
		searchQuery = mq
	}

	var filterQueries []query.Query
	for k, v := range r.URL.Query() {
		if strings.HasPrefix(k, "filter_") && len(v) > 0 {
			metaKey := strings.TrimPrefix(k, "filter_")
			// Bleve nested fields are separated by dot: Metadata.field
			fq := bleve.NewMatchQuery(v[0])
			fq.SetField("metadata." + metaKey)
			filterQueries = append(filterQueries, fq)
		}
	}

	if len(filterQueries) > 0 {
		filterQueries = append([]query.Query{searchQuery}, filterQueries...)
		searchQuery = bleve.NewConjunctionQuery(filterQueries...)
	}

	searchReq := bleve.NewSearchRequest(searchQuery)
	searchReq.Size = 10
	searchReq.Fields = []string{"*"}

	// Enable highlighting if requested.
	highlight := r.URL.Query().Get("highlight") == "true"
	if highlight {
		searchReq.Highlight = bleve.NewHighlightWithStyle(ansi.Name)
	}

	res, err := index.Search(searchReq)
	if err != nil {
		http.Error(w, "search failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	results := make([]SearchResult, 0, len(res.Hits))
	for _, hit := range res.Hits {
		title, _ := hit.Fields["title"].(string)
		content, _ := hit.Fields["content"].(string)

		var emb []float64
		if val, ok := docEmbeddings.Load(hit.ID); ok {
			emb = val.([]float64)
		}

		metadata := make(map[string]string)
		for k, v := range hit.Fields {
			if strings.HasPrefix(k, "metadata.") {
				metaKey := strings.TrimPrefix(k, "metadata.")
				if strVal, ok := v.(string); ok {
					metadata[metaKey] = strVal
				}
			}
		}

		sr := SearchResult{
			ID:        hit.ID,
			Title:     title,
			Content:   content,
			Score:     hit.Score,
			Embedding: emb,
			Metadata:  metadata,
		}

		if highlight && len(hit.Fragments) > 0 {
			sr.Highlights = make(map[string]string)
			for field, frags := range hit.Fragments {
				if len(frags) > 0 {
					sr.Highlights[field] = frags[0]
				}
			}
		}

		results = append(results, sr)
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

// ---------------------------------------------------------------------------
// Request Logging & Metrics (shard-level)
// ---------------------------------------------------------------------------

var (
	shardTotalRequests int64
	shardSearchCount   int64
	shardIndexCount    int64
)

// shardStatusWriter captures the response status code.
type shardStatusWriter struct {
	http.ResponseWriter
	code int
}

func (sw *shardStatusWriter) WriteHeader(code int) {
	sw.code = code
	sw.ResponseWriter.WriteHeader(code)
}

// logRequest is shard-level request logging middleware.
func logRequest(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &shardStatusWriter{ResponseWriter: w, code: http.StatusOK}

		atomic.AddInt64(&shardTotalRequests, 1)
		path := r.URL.Path
		switch {
		case path == "/search":
			atomic.AddInt64(&shardSearchCount, 1)
		case path == "/index" || path == "/bulk":
			atomic.AddInt64(&shardIndexCount, 1)
		}

		next(sw, r)

		log.Printf("[%s] HTTP %s %s | %d | %dms",
			shardID, r.Method, r.URL.Path, sw.code,
			time.Since(start).Milliseconds(),
		)
	}
}

var internalSecret string

// internalAuthMiddleware rejects requests that don't have the correct X-Internal-Secret.
func internalAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if internalSecret == "" {
			next(w, r)
			return
		}
		if r.Header.Get("X-Internal-Secret") != internalSecret {
			http.Error(w, `{"error":"unauthorized cluster access"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// wrapShardMiddleware chains: authenticaton -> logging -> handler
func wrapShardMiddleware(handler http.HandlerFunc) http.HandlerFunc {
	return internalAuthMiddleware(logRequest(handler))
}

// shardMetricsHandler returns shard Prometheus-compatible metrics.
func shardMetricsHandler(w http.ResponseWriter, r *http.Request) {
	docCount, _ := index.DocCount()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	fmt.Fprintf(w, "# HELP shard_requests_total Total HTTP requests to this shard.\n")
	fmt.Fprintf(w, "# TYPE shard_requests_total counter\n")
	fmt.Fprintf(w, "shard_requests_total{shard=\"%s\"} %d\n\n", shardID, atomic.LoadInt64(&shardTotalRequests))

	fmt.Fprintf(w, "# HELP shard_search_requests Search requests to this shard.\n")
	fmt.Fprintf(w, "# TYPE shard_search_requests counter\n")
	fmt.Fprintf(w, "shard_search_requests{shard=\"%s\"} %d\n\n", shardID, atomic.LoadInt64(&shardSearchCount))

	fmt.Fprintf(w, "# HELP shard_index_requests Index requests to this shard.\n")
	fmt.Fprintf(w, "# TYPE shard_index_requests counter\n")
	fmt.Fprintf(w, "shard_index_requests{shard=\"%s\"} %d\n\n", shardID, atomic.LoadInt64(&shardIndexCount))

	fmt.Fprintf(w, "# HELP shard_docs_ingested Total documents ingested.\n")
	fmt.Fprintf(w, "# TYPE shard_docs_ingested counter\n")
	fmt.Fprintf(w, "shard_docs_ingested{shard=\"%s\"} %d\n\n", shardID, atomic.LoadInt64(&docsIngested))

	fmt.Fprintf(w, "# HELP shard_docs_deleted Total documents deleted.\n")
	fmt.Fprintf(w, "# TYPE shard_docs_deleted counter\n")
	fmt.Fprintf(w, "shard_docs_deleted{shard=\"%s\"} %d\n\n", shardID, atomic.LoadInt64(&docsDeleted))

	fmt.Fprintf(w, "# HELP shard_doc_count Current documents in index.\n")
	fmt.Fprintf(w, "# TYPE shard_doc_count gauge\n")
	fmt.Fprintf(w, "shard_doc_count{shard=\"%s\"} %d\n\n", shardID, docCount)

	fmt.Fprintf(w, "# HELP shard_uptime_seconds Shard uptime.\n")
	fmt.Fprintf(w, "# TYPE shard_uptime_seconds gauge\n")
	fmt.Fprintf(w, "shard_uptime_seconds{shard=\"%s\"} %.0f\n", shardID, time.Since(shardStartTime).Seconds())
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
	dataDir := getenv("DATA_DIR", "./data")
	internalSecret = getenv("INTERNAL_SECRET", "")
	if internalSecret != "" {
		log.Printf("[%s] internal cluster authentication enabled", shardID)
	} else {
		log.Printf("[%s] WARNING: INTERNAL_SECRET not set - shard is unprotected", shardID)
	}

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("[%s] failed to create data directory %s: %v", shardID, dataDir, err)
	}

	embeddingsPath := filepath.Join(dataDir, "embeddings.gob")
	if err := loadEmbeddings(embeddingsPath); err != nil {
		log.Printf("[%s] failed to load embeddings: %v", shardID, err)
	} else {
		log.Printf("[%s] loaded embeddings from %s", shardID, embeddingsPath)
	}

	docs := seedDocuments(shardID)
	idx, err := buildIndex(dataDir, docs)
	if err != nil {
		log.Fatalf("[%s] failed to build/open index: %v", shardID, err)
	}
	index = idx

	http.HandleFunc("/search", wrapShardMiddleware(searchHandler))
	http.HandleFunc("/health", wrapShardMiddleware(healthHandler))
	http.HandleFunc("/index", wrapShardMiddleware(ingestHandler))
	http.HandleFunc("/stats", wrapShardMiddleware(statsHandler))
	http.HandleFunc("/metrics", wrapShardMiddleware(shardMetricsHandler))
	http.HandleFunc("/documents/", wrapShardMiddleware(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getDocumentHandler(w, r)
		case http.MethodDelete:
			deleteHandler(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	http.HandleFunc("/bulk", wrapShardMiddleware(bulkIngestHandler))

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

	// Save embeddings cache on shutdown.
	if err := saveEmbeddings(embeddingsPath); err != nil {
		log.Printf("[%s] failed to save embeddings: %v", shardID, err)
	} else {
		log.Printf("[%s] saved embeddings to %s", shardID, embeddingsPath)
	}

	// Close index cleanly so it's ready for the next restart.
	index.Close()
	log.Printf("[%s] shutdown complete", shardID)
}
