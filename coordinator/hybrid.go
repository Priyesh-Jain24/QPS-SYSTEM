package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// TriggerHybridCrawl initiates a lightweight crawl for a given topic
func TriggerHybridCrawl(topic string, limit int) {
	log.Printf("Starting hybrid crawler for topic: '%s', limit=%d.", topic, limit)

	startingURLs := searchTopicOnWikipedia(topic)
	if len(startingURLs) == 0 {
		log.Printf("Hybrid crawl failed: No Wikipedia articles found for '%s'", topic)
		return
	}

	queue := make(chan string, 100)
	visited := make(map[string]bool)
	var mu sync.Mutex

	var workersWg sync.WaitGroup
	var tasksWg sync.WaitGroup
	count := 0

	// Start 3 workers
	for w := 1; w <= 3; w++ {
		workersWg.Add(1)
		go func(workerID int) {
			defer workersWg.Done()
			for target := range queue {
				mu.Lock()
				if count >= limit {
					mu.Unlock()
					tasksWg.Done()
					continue
				}
				count++
				currentCount := count
				mu.Unlock()

				log.Printf("[Hybrid-W%d] Crawling %d/%d: %s", workerID, currentCount, limit, target)
				outlinks := crawlPage(target)

				// Enqueue found links if not visited
				mu.Lock()
				for _, link := range outlinks {
					if !visited[link] && count+len(queue) < limit {
						visited[link] = true
						tasksWg.Add(1)
						queue <- link
					}
				}
				mu.Unlock()

				tasksWg.Done()
			}
		}(w)
	}

	// Seed the queue
	for _, u := range startingURLs {
		visited[u] = true
		tasksWg.Add(1)
		queue <- u
	}

	doneWait := make(chan struct{})
	go func() {
		tasksWg.Wait()
		close(doneWait)
	}()

	// Wait for all tasks to be gracefully completed or limit reached
waitLoop:
	for {
		mu.Lock()
		cnt := count
		mu.Unlock()
		if cnt >= limit {
			break waitLoop
		}

		select {
		case <-doneWait:
			break waitLoop
		case <-time.After(50 * time.Millisecond):
		}

		select {
		case <-doneWait:
			break waitLoop
		default:
		}

		if cnt >= limit {
			break waitLoop
		}
	}

	close(queue)
	workersWg.Wait()
	log.Printf("Hybrid crawler finished gracefully for '%s'.", topic)
}

func searchTopicOnWikipedia(topic string) []string {
	apiURL := fmt.Sprintf("https://en.wikipedia.org/w/api.php?action=opensearch&search=%s&limit=5&format=json", url.QueryEscape(topic))
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "SearchSphere Hybrid Crawler / 1.0")

	client := &http.Client{Timeout: 45 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer res.Body.Close()

	var payload []interface{}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil
	}

	if len(payload) < 4 {
		return nil
	}

	urlsArray, ok := payload[3].([]interface{})
	if !ok {
		return nil
	}

	var results []string
	for _, u := range urlsArray {
		if str, ok := u.(string); ok {
			results = append(results, str)
		}
	}
	return results
}

func crawlPage(target string) []string {
	outlinks := []string{}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequest("GET", target, nil)
	if err != nil {
		return outlinks
	}
	req.Header.Set("User-Agent", "SearchSphere Hybrid Crawler / 1.0")

	res, err := client.Do(req)
	if err != nil {
		return outlinks
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Printf("Crawl failed: %s got HTTP %d", target, res.StatusCode)
		return outlinks
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Printf("Crawl parse failed: %s: %v", target, err)
		return outlinks
	}

	title := strings.TrimSpace(doc.Find("title").First().Text())

	var sb strings.Builder
	doc.Find("p").Each(func(i int, s *goquery.Selection) {
		text := strings.TrimSpace(s.Text())
		if len(text) > 20 {
			sb.WriteString(text)
			sb.WriteString("\n\n")
		}
	})
	content := strings.TrimSpace(sb.String())

	if title == "" || content == "" {
		log.Printf("Crawl dropped %s: title='%s', content_len=%d", target, title, len(content))
		return outlinks
	}

	parsedURL, _ := url.Parse(target)
	domain := parsedURL.Host

	// Push directly to the appropriate shard
	pushToShard(target, title, content, domain)

	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists {
			linkURL, err := url.Parse(href)
			if err == nil {
				absURL := parsedURL.ResolveReference(linkURL)
				absURL.Fragment = ""
				urlStr := absURL.String()
				if strings.HasPrefix(urlStr, "http") {
					outlinks = append(outlinks, urlStr)
				}
			}
		}
	})

	return outlinks
}

func pushToShard(sourceURL, title, content, domain string) {
	hash := md5.Sum([]byte(sourceURL))
	id := fmt.Sprintf("web-%x", hash)

	body, _ := json.Marshal(map[string]interface{}{
		"id":      id,
		"title":   title,
		"content": content,
		"metadata": map[string]string{
			"url":    sourceURL,
			"domain": domain,
			"source": "hybrid-crawler",
		},
	})

	// Pick a shard using the consistent hash ring
	shards := shardRegistry.List()
	if len(shards) == 0 {
		log.Printf("Hybrid crawler: no shards available to index %s", sourceURL)
		return
	}

	targetShard := shards[0]
	if hashRing != nil {
		targetAddr := hashRing.GetShard(id)
		for _, s := range shards {
			if s.Addr == targetAddr {
				targetShard = s
				break
			}
		}
	}

	shardURL := fmt.Sprintf("http://%s/index", targetShard.Addr)
	req, err := http.NewRequest("POST", shardURL, bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Hybrid crawler: failed to create request for %s: %v", sourceURL, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if internalSecret != "" {
		req.Header.Set("X-Internal-Secret", internalSecret)
	}

	resp, err := shardClient.Do(req)
	if err != nil {
		log.Printf("Hybrid crawler: failed to index %s to shard %s: %v", sourceURL, targetShard.ID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		log.Printf("Hybrid Indexed: %s -> %s", title, targetShard.ID)
	} else {
		log.Printf("Hybrid index %s to shard %s failed: HTTP %d", sourceURL, targetShard.ID, resp.StatusCode)
	}
}
