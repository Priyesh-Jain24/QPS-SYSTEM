package main

import (
	"encoding/json"
	"hash/fnv"
	"io"
	"log"
	"math/bits"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/v2"
)

var (
	shardStartTime time.Time
	docsIngested   int64
	docsDeleted    int64
	seenSimHashes  sync.Map // uint64 -> docID
)

// simhash computes a 64-bit document fingerprint where similar texts map to similar hashes.
func simhash(text string) uint64 {
	words := strings.Fields(text)
	var v [64]int
	for _, word := range words {
		if len(word) < 3 {
			continue // skip stop-words for stability
		}
		h := fnv.New64a()
		h.Write([]byte(strings.ToLower(word)))
		hash := h.Sum64()
		for i := 0; i < 64; i++ {
			if ((hash >> i) & 1) == 1 {
				v[i]++
			} else {
				v[i]--
			}
		}
	}
	var fingerprint uint64
	for i := 0; i < 64; i++ {
		if v[i] > 0 {
			fingerprint |= (1 << i)
		}
	}
	return fingerprint
}

// isDuplicate returns true if the content is a near-duplicate (>95% match) of an existing doc.
func isDuplicate(id, content string) (bool, string) {
	sh := simhash(content)
	duplicate := false
	dupID := ""

	seenSimHashes.Range(func(key, value interface{}) bool {
		knownHash := key.(uint64)
		knownID := value.(string)
		if knownID == id { // Allow upserts of self
			seenSimHashes.Store(sh, id) // update hash just in case
			return true
		}
		// Hamming distance <= 2 means out of 64 bits only 2 differ (near identical)
		if bits.OnesCount64(sh^knownHash) <= 2 {
			duplicate = true
			dupID = knownID
			return false
		}
		return true
	})

	if !duplicate {
		seenSimHashes.Store(sh, id)
	}
	return duplicate, dupID
}

// IngestRequest is the payload accepted by POST /index.
type IngestRequest struct {
	ID       string            `json:"id"`
	Title    string            `json:"title"`
	Content  string            `json:"content"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// IngestResponse is the payload returned after indexing a document.
type IngestResponse struct {
	Status  string `json:"status"`
	ShardID string `json:"shard_id"`
	DocID   string `json:"doc_id"`
}

// BulkIngestRequest is the payload accepted by POST /bulk.
type BulkIngestRequest struct {
	Documents []IngestRequest `json:"documents"`
}

// BulkIngestResponse is the payload returned after bulk indexing.
type BulkIngestResponse struct {
	Status  string   `json:"status"`
	ShardID string   `json:"shard_id"`
	Count   int      `json:"count"`
	Errors  []string `json:"errors,omitempty"`
}

// StatsResponse is the payload returned by GET /stats.
type StatsResponse struct {
	ShardID      string `json:"shard_id"`
	DocCount     int    `json:"doc_count"`
	DocsIngested int64  `json:"docs_ingested"`
	DocsDeleted  int64  `json:"docs_deleted"`
	UptimeSecs   int64  `json:"uptime_seconds"`
}

// ingestHandler accepts a JSON document and indexes it in Bleve.
//
//	POST /index
//	Body: {"id": "...", "title": "...", "content": "..."}
func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MB limit
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req IngestRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.ID == "" {
		http.Error(w, "document 'id' is required", http.StatusBadRequest)
		return
	}
	if req.Title == "" && req.Content == "" {
		http.Error(w, "document must have at least 'title' or 'content'", http.StatusBadRequest)
		return
	}

	if dup, originalID := isDuplicate(req.ID, req.Content); dup {
		log.Printf("[%s] rejected duplicate of %s (ID: %s)", shardID, originalID, req.ID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(IngestResponse{
			Status:  "skipped_duplicate",
			ShardID: shardID,
			DocID:   req.ID,
		})
		return
	}

	chunks, _ := getDocumentChunks(req.ID, req.Title, req.Content)
	for _, c := range chunks {
		meta := make(map[string]string)
		for k, v := range req.Metadata {
			meta[k] = v
		}
		meta["parent_id"] = req.ID

		doc := Document{
			ID:       c.ID,
			Title:    req.Title,
			Content:  c.Text,
			Metadata: meta,
		}

		if err := index.Index(doc.ID, doc); err != nil {
			http.Error(w, "indexing failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if len(c.Vector) > 0 {
			docEmbeddings.Store(doc.ID, c.Vector)
		}
	}

	atomic.AddInt64(&docsIngested, 1)
	log.Printf("[%s] indexed document %q", shardID, req.ID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(IngestResponse{
		Status:  "indexed",
		ShardID: shardID,
		DocID:   req.ID,
	})
}

// deleteHandler removes a document from the Bleve index and embedding cache.
//
//	DELETE /documents/{id}
func deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract ID from path: /documents/{id}
	docID := strings.TrimPrefix(r.URL.Path, "/documents/")
	if docID == "" {
		http.Error(w, "document ID is required in path", http.StatusBadRequest)
		return
	}

	// Check if the document exists first.
	bleveDoc, err := index.Document(docID)
	if err != nil || bleveDoc == nil {
		http.Error(w, "document not found", http.StatusNotFound)
		return
	}

	if err := index.Delete(docID); err != nil {
		http.Error(w, "delete failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	docEmbeddings.Delete(docID)
	atomic.AddInt64(&docsDeleted, 1)
	log.Printf("[%s] deleted document %q", shardID, docID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "deleted",
		"shard_id": shardID,
		"doc_id":   docID,
	})
}

// getDocumentHandler retrieves a single document by ID from the Bleve index.
//
//	GET /documents/{id}
func getDocumentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	docID := strings.TrimPrefix(r.URL.Path, "/documents/")
	if docID == "" {
		http.Error(w, "document ID is required in path", http.StatusBadRequest)
		return
	}

	// Use a DocID query for exact ID lookup to reconstruct stored fields.
	q := bleve.NewDocIDQuery([]string{docID})
	searchReq := bleve.NewSearchRequest(q)
	searchReq.Size = 1
	searchReq.Fields = []string{"title", "content"}

	res, err := index.Search(searchReq)
	if err != nil {
		http.Error(w, "lookup failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if res.Total == 0 {
		http.Error(w, "document not found", http.StatusNotFound)
		return
	}

	hit := res.Hits[0]
	title, _ := hit.Fields["title"].(string)
	content, _ := hit.Fields["content"].(string)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":       docID,
		"title":    title,
		"content":  content,
		"shard_id": shardID,
	})
}

// bulkIngestHandler accepts an array of documents and indexes them.
//
//	POST /bulk
//	Body: {"documents": [{id, title, content}, ...]}
func bulkIngestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20)) // 10 MB limit for bulk
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req BulkIngestRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Documents) == 0 {
		http.Error(w, "'documents' array is required and must not be empty", http.StatusBadRequest)
		return
	}

	var (
		mu        sync.Mutex
		errors    []string
		indexed   int
		wg        sync.WaitGroup
		semaphore = make(chan struct{}, 5) // limit concurrency for embedding calls
	)

	for _, d := range req.Documents {
		if d.ID == "" {
			mu.Lock()
			errors = append(errors, "skipped document with empty ID")
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func(d IngestRequest) {
			defer wg.Done()

			if dup, _ := isDuplicate(d.ID, d.Content); dup {
				mu.Lock()
				// Silently skip duplicate instead of returning an error for bulk
				indexed++
				mu.Unlock()
				return
			}

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			chunks, _ := getDocumentChunks(d.ID, d.Title, d.Content)
			for _, c := range chunks {
				meta := make(map[string]string)
				for k, v := range d.Metadata {
					meta[k] = v
				}
				meta["parent_id"] = d.ID

				doc := Document{ID: c.ID, Title: d.Title, Content: c.Text, Metadata: meta}
				if err := index.Index(doc.ID, doc); err != nil {
					mu.Lock()
					errors = append(errors, "failed to index "+doc.ID+": "+err.Error())
					mu.Unlock()
					continue // try next chunk
				}

				if len(c.Vector) > 0 {
					docEmbeddings.Store(doc.ID, c.Vector)
				}
			}

			atomic.AddInt64(&docsIngested, 1)
			mu.Lock()
			indexed++
			mu.Unlock()
		}(d)
	}

	wg.Wait()
	log.Printf("[%s] bulk indexed %d/%d documents", shardID, indexed, len(req.Documents))

	status := http.StatusCreated
	if len(errors) > 0 && indexed == 0 {
		status = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(BulkIngestResponse{
		Status:  "indexed",
		ShardID: shardID,
		Count:   indexed,
		Errors:  errors,
	})
}

// statsHandler returns runtime statistics for this shard.
//
//	GET /stats
func statsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	docCount, _ := index.DocCount()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(StatsResponse{
		ShardID:      shardID,
		DocCount:     int(docCount),
		DocsIngested: atomic.LoadInt64(&docsIngested),
		DocsDeleted:  atomic.LoadInt64(&docsDeleted),
		UptimeSecs:   int64(time.Since(shardStartTime).Seconds()),
	})
}
