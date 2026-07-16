package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"
)

var ollamaURL string

type ollamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type ollamaResponse struct {
	Embedding []float64 `json:"embedding"`
}

// getEmbedding fetches a vector embedding from the local Ollama instance.
func getEmbedding(ctx context.Context, text string) ([]float64, error) {
	if ollamaURL == "" {
		return nil, nil // Re-ranking disabled
	}

	reqBody, _ := json.Marshal(ollamaRequest{
		Model:  "all-minilm", // Must be pulled prior via: ollama run all-minilm
		Prompt: text,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ollamaURL+"/api/embeddings", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Use shardClient as it already has tight timeouts configured.
	resp, err := shardClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var ores ollamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&ores); err != nil {
		return nil, err
	}
	return ores.Embedding, nil
}

// cosineSimilarity calculates the mathematical distance between two vectors.
// Returns a value between -1 and 1.
func cosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dotProduct, normA, normB float64
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

// Rerank queries Ollama for the semantic embedding of the query and
// computes a hybrid score (bleve + cosine_sim) utilizing the pre-calculated
// document embeddings returned from the shards.
func Rerank(query string, results []MergedResult) []MergedResult {
	if ollamaURL == "" || len(results) == 0 {
		return results
	}

	// 1. Parse hybrid weight config
	weightStr := os.Getenv("HYBRID_WEIGHT")
	if weightStr == "" {
		weightStr = "0.7" // 70% Semantic, 30% Keyword
	}
	semanticWeight, err := strconv.ParseFloat(weightStr, 64)
	if err != nil || semanticWeight < 0 || semanticWeight > 1 {
		semanticWeight = 0.7
	}
	keywordWeight := 1.0 - semanticWeight

	// 2. Get query embedding
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	qEmb, err := getEmbedding(ctx, query)
	if err != nil || len(qEmb) == 0 {
		log.Printf("reranker: failed to get query embedding: %v", err)
		return results
	}

	// 3. Compute cosine similarity synchronously (no network I/O needed anymore!)
	for i := range results {
		sim := cosineSimilarity(qEmb, results[i].Embedding)

		// Map bleve score to a smaller scale so semantic sim dominates if configured
		mappedBleveScore := results[i].Score * 0.1

		results[i].Score = (mappedBleveScore * keywordWeight) + (sim * semanticWeight)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results
}
