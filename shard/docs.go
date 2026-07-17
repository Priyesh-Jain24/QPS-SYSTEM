package main

// Document is the unit indexed and returned by a shard.
type Document struct {
	ID       string            `json:"id"`
	Title    string            `json:"title"`
	Content  string            `json:"content"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// seedDocuments returns a small set of sample documents so each shard has
// something to search over out of the box. In a real deployment these
// would be loaded from a database, file, or ingestion pipeline, and each
// shard would hold a different slice of the overall corpus.
func seedDocuments(shardID string) []Document {
	return []Document{
		{
			ID:      shardID + "-doc-1",
			Title:   "Introduction to Go",
			Content: "Go is a statically typed, compiled programming language designed at Google. It is known for simplicity and strong support for concurrency.",
		},
		{
			ID:      shardID + "-doc-2",
			Title:   "Understanding Distributed Systems",
			Content: "Distributed systems consist of multiple independent nodes that communicate over a network to achieve a common goal, such as fault tolerance and scalability.",
		},
		{
			ID:      shardID + "-doc-3",
			Title:   "Full-Text Search with Bleve",
			Content: "Bleve is a modern text indexing library for Go. It supports full-text search, faceting, and custom analyzers similar to Lucene or Elasticsearch.",
		},
		{
			ID:      shardID + "-doc-4",
			Title:   "Caching Strategies with Redis",
			Content: "Redis is an in-memory data store often used as a cache to reduce latency and load on backend systems by storing frequently accessed results.",
		},
		{
			ID:      shardID + "-doc-5",
			Title:   "Service Discovery with etcd",
			Content: "etcd is a distributed key-value store used for service discovery and configuration management, commonly paired with leases for health tracking.",
		},
	}
}
