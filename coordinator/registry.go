package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type ShardInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// ShardRegistry keeps an in-memory, concurrency-safe view of live shards,
// kept up to date by watching etcd's /shards/ prefix.
type ShardRegistry struct {
	mu     sync.RWMutex
	shards map[string]ShardInfo // key: etcd key, e.g. "/shards/shard-1"
}

func NewShardRegistry() *ShardRegistry {
	return &ShardRegistry{shards: make(map[string]ShardInfo)}
}

func (r *ShardRegistry) List() []ShardInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ShardInfo, 0, len(r.shards))
	for _, s := range r.shards {
		out = append(out, s)
	}
	return out
}

// Count returns the number of currently registered shards.
func (r *ShardRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.shards)
}

func (r *ShardRegistry) Watch(ctx context.Context, cli *clientv3.Client) error {
	resp, err := cli.Get(ctx, "/shards/", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	r.mu.Lock()
	for _, kv := range resp.Kvs {
		var info ShardInfo
		if err := json.Unmarshal(kv.Value, &info); err == nil {
			r.shards[string(kv.Key)] = info
			log.Printf("shard found at startup: %s (%s)", info.ID, info.Addr)
		}
	}
	r.mu.Unlock()

	go r.watchLoop(ctx, cli, resp.Header.Revision+1)
	return nil
}

// watchLoop continuously watches etcd for shard changes. On errors or
// channel closures it reconnects with exponential backoff (1s → 2s → 4s,
// capped at 30s) rather than silently dying.
func (r *ShardRegistry) watchLoop(ctx context.Context, cli *clientv3.Client, startRev int64) {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	rev := startRev

	for {
		select {
		case <-ctx.Done():
			log.Println("shard watch stopped (context cancelled)")
			return
		default:
		}

		watchCh := cli.Watch(ctx, "/shards/", clientv3.WithPrefix(), clientv3.WithRev(rev))

		for wresp := range watchCh {
			if wresp.Err() != nil {
				log.Printf("etcd watch error: %v, reconnecting in %v", wresp.Err(), backoff)
				break
			}

			// Reset backoff on successful event.
			backoff = 1 * time.Second

			for _, ev := range wresp.Events {
				key := string(ev.Kv.Key)
				switch ev.Type {
				case clientv3.EventTypePut:
					var info ShardInfo
					if err := json.Unmarshal(ev.Kv.Value, &info); err == nil {
						r.mu.Lock()
						r.shards[key] = info
						r.mu.Unlock()
						log.Printf("shard registered: %s (%s)", info.ID, info.Addr)
					}
				case clientv3.EventTypeDelete:
					r.mu.Lock()
					delete(r.shards, key)
					r.mu.Unlock()
					log.Printf("shard deregistered: %s", key)
				}
				rev = ev.Kv.ModRevision + 1
			}
		}

		// watchCh was closed — reconnect after backoff.
		log.Printf("etcd watch channel closed, reconnecting in %v", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}
