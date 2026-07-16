package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

var (
	shardStartTime time.Time
	docsIngested   int64
)

// IngestRequest is the payload accepted by POST /index.
type IngestRequest struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

// IngestResponse is the payload returned after indexing a document.
type IngestResponse struct {
	Status  string `json:"status"`
	ShardID string `json:"shard_id"`
	DocID   string `json:"doc_id"`
}

// StatsResponse is the payload returned by GET /stats.
type StatsResponse struct {
	ShardID      string `json:"shard_id"`
	DocCount     int    `json:"doc_count"`
	DocsIngested int64  `json:"docs_ingested"`
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

	doc := Document{
		ID:      req.ID,
		Title:   req.Title,
		Content: req.Content,
	}

	if err := index.Index(doc.ID, doc); err != nil {
		http.Error(w, "indexing failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if emb, err := getEmbedding(doc.Title + " " + doc.Content); err == nil && len(emb) > 0 {
		docEmbeddings.Store(doc.ID, emb)
	}

	atomic.AddInt64(&docsIngested, 1)
	log.Printf("[%s] indexed document %q", shardID, doc.ID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(IngestResponse{
		Status:  "indexed",
		ShardID: shardID,
		DocID:   doc.ID,
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
		UptimeSecs:   int64(time.Since(shardStartTime).Seconds()),
	})
}
