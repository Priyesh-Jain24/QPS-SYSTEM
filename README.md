# SearchSphere – Distributed Search Engine

A distributed full-text search engine built with Go, Bleve, Redis, and etcd.

## Current Phase: 1 & 2

This base project implements:
- **Shard service** (`/shard`): a standalone Go service that indexes documents
  locally using Bleve and exposes a `/search` HTTP endpoint.
- **Coordinator service** (`/coordinator`): fans a query out concurrently to a
  static list of shards, merges + ranks the results, and optionally caches
  results in Redis.
- **Docker Compose** setup that wires up 3 shards, the coordinator, Redis,
  and an etcd node (etcd is included for Phase 3 but not yet used by the
  Go code — the coordinator still uses a static shard list via env vars).

## Project Structure

```
searchsphere/
├── coordinator/
│   ├── main.go        # Fan-out, merge, cache logic
│   ├── go.mod
│   └── Dockerfile
├── shard/
│   ├── main.go         # Bleve index + /search + /health
│   ├── go.mod
│   ├── Dockerfile
│   └── docs.go          # sample seed documents
├── data/
│   └── sample_docs.json # extra sample docs you can load/experiment with
├── docker-compose.yml
└── README.md
```

## Running locally (without Docker)

You'll need Go 1.21+ installed.

### 1. Start a shard

```bash
cd shard
go mod tidy
SHARD_ID=shard1 PORT=8081 go run .
```

Open a second terminal and start another shard on a different port:

```bash
SHARD_ID=shard2 PORT=8082 go run .
```

### 2. Start the coordinator

```bash
cd coordinator
go mod tidy
SHARD_ADDRS=http://localhost:8081,http://localhost:8082 \
REDIS_ADDR=localhost:6379 \
PORT=8080 go run .
```

If Redis isn't running, the coordinator will log a warning and simply skip
caching — it won't crash.

### 3. Query it

```bash
curl "http://localhost:8080/search?q=golang"
```

## Running with Docker Compose

```bash
docker compose up --build
```

This starts 3 shards, the coordinator, Redis, and etcd. Query the
coordinator at `http://localhost:8080/search?q=...`.

## Next steps (not yet implemented)

- **Phase 3**: shards self-register in etcd with a TTL lease; coordinator
  watches etcd instead of reading a static `SHARD_ADDRS` env var.
- **Phase 4**: tune Redis cache key normalization + invalidation.
- **Phase 6**: semantic re-ranking of top-K results using embeddings
  (Ollama / Sentence Transformers).
