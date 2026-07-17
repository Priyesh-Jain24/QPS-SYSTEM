package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// trackQuery records a search query for analytics:
// 1. Increments its score in the "searchsphere:popular" sorted set (frequency ranking).
// 2. Pushes it onto the "searchsphere:recent" list (capped at 100 entries).
func trackQuery(query string, resultCount int, latencyMs int64) {
	if rdb == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	normalized := strings.ToLower(strings.TrimSpace(query))
	if normalized == "" {
		return
	}

	// Increment popularity score.
	rdb.ZIncrBy(ctx, "searchsphere:popular", 1, normalized)

	// Push to recent queries list (capped at 100).
	entry, _ := json.Marshal(map[string]interface{}{
		"query":        normalized,
		"result_count": resultCount,
		"latency_ms":   latencyMs,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	})
	rdb.LPush(ctx, "searchsphere:recent", string(entry))
	rdb.LTrim(ctx, "searchsphere:recent", 0, 99)
}

// suggestHandler returns autocomplete suggestions by matching a prefix
// against the popular queries sorted set.
//
//	GET /suggest?q=prefix
func suggestHandler(w http.ResponseWriter, r *http.Request) {
	prefix := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	if prefix == "" {
		http.Error(w, "missing required query param 'q'", http.StatusBadRequest)
		return
	}

	if rdb == nil {
		writeJSON(w, map[string]interface{}{
			"prefix":      prefix,
			"suggestions": []string{},
			"source":      "disabled",
		})
		return
	}

	ctx := r.Context()

	// Get all members from the sorted set and filter by prefix.
	// For production scale, a trie or Redis search module would be better,
	// but for our use case ZRANGEBYSCORE + client-side filter is fine.
	members, err := rdb.ZRevRangeWithScores(ctx, "searchsphere:popular", 0, -1).Result()
	if err != nil {
		log.Printf("suggest: redis error: %v", err)
		writeJSON(w, map[string]interface{}{
			"prefix":      prefix,
			"suggestions": []string{},
		})
		return
	}

	type suggestion struct {
		Query string  `json:"query"`
		Score float64 `json:"score"`
	}

	suggestions := make([]suggestion, 0, 10)
	for _, m := range members {
		q := m.Member.(string)
		if strings.HasPrefix(q, prefix) && q != prefix {
			suggestions = append(suggestions, suggestion{
				Query: q,
				Score: m.Score,
			})
			if len(suggestions) >= 10 {
				break
			}
		}
	}

	writeJSON(w, map[string]interface{}{
		"prefix":      prefix,
		"suggestions": suggestions,
	})
}

// analyticsHandler returns search analytics: top popular queries and recent queries.
//
//	GET /analytics
func analyticsHandler(w http.ResponseWriter, r *http.Request) {
	if rdb == nil {
		writeJSON(w, map[string]interface{}{
			"status": "analytics disabled (no Redis)",
		})
		return
	}

	ctx := r.Context()

	// Top 10 popular queries by frequency.
	type popularQuery struct {
		Query string  `json:"query"`
		Count float64 `json:"count"`
	}

	topMembers, _ := rdb.ZRevRangeWithScores(ctx, "searchsphere:popular", 0, 9).Result()
	popular := make([]popularQuery, 0, len(topMembers))
	for _, m := range topMembers {
		popular = append(popular, popularQuery{
			Query: m.Member.(string),
			Count: m.Score,
		})
	}

	// Last 20 recent queries.
	recentRaw, _ := rdb.LRange(ctx, "searchsphere:recent", 0, 19).Result()
	recent := make([]json.RawMessage, 0, len(recentRaw))
	for _, r := range recentRaw {
		recent = append(recent, json.RawMessage(r))
	}

	writeJSON(w, map[string]interface{}{
		"popular_queries": popular,
		"recent_queries":  recent,
		"total_searches":  atomic.LoadInt64(&metricsSearchRequests),
	})
}
