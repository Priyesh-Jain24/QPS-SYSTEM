package main

import (
	"context"
	"encoding/json"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ShardInfo is what gets written to etcd for each shard.
type ShardInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"` // e.g. "shard1:8081" — reachable via the docker network
}

// RegisterShard puts this shard's info into etcd under a lease and starts
// a background keepalive loop. It returns the lease ID so the caller can
// revoke it explicitly on graceful shutdown (instead of waiting for TTL expiry).
func RegisterShard(ctx context.Context, cli *clientv3.Client, info ShardInfo, ttlSeconds int64) (clientv3.LeaseID, error) {
	lease, err := cli.Grant(ctx, ttlSeconds)
	if err != nil {
		return 0, err
	}

	data, err := json.Marshal(info)
	if err != nil {
		return 0, err
	}

	key := "/shards/" + info.ID
	if _, err := cli.Put(ctx, key, string(data), clientv3.WithLease(lease.ID)); err != nil {
		return 0, err
	}

	keepAliveCh, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return 0, err
	}

	// Must drain the channel or the lease won't actually renew.
	go func() {
		for {
			select {
			case _, ok := <-keepAliveCh:
				if !ok {
					log.Printf("[%s] etcd keepalive channel closed", info.ID)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Printf("[%s] registered in etcd at %s -> %s", info.ID, key, info.Addr)
	return lease.ID, nil
}