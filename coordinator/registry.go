package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

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

	watchCh := cli.Watch(ctx, "/shards/", clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
	go func() {
		for wresp := range watchCh {
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
			}
		}
	}()
	return nil
}