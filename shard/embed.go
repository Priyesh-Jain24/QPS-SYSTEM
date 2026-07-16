package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
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
// Using a 5s timeout to avoid hanging the indexing process endlessly.
func getEmbedding(text string) ([]float64, error) {
	if ollamaURL == "" {
		return nil, nil // embeddings disabled
	}

	reqBody, _ := json.Marshal(ollamaRequest{
		Model:  "all-minilm",
		Prompt: text,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ollamaURL+"/api/embeddings", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("[%s] ollama returned non-200 status: %d", shardID, resp.StatusCode)
		return nil, nil // Gracefully fallback to no embedding
	}

	var ores ollamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&ores); err != nil {
		return nil, err
	}
	return ores.Embedding, nil
}
