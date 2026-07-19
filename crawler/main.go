package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// SearchSphere endpoints and secrets
const SearchSphereIndexURL = "http://localhost:8080/index"
const SearchSphereAPIKey = "supersecret-admin-key"

type IngestRequest struct {
	ID       string            `json:"id"`
	Title    string            `json:"title"`
	Content  string            `json:"content"`
	Metadata map[string]string `json:"metadata"`
}

func main() {
	topic := flag.String("topic", "", "Natural language topic to automatically search (e.g. 'World Cup')")
	startURL := flag.String("url", "https://go.dev/doc/", "Starting URL to crawl (used if topic is empty)")
	limit := flag.Int("limit", 15, "Maximum number of pages to crawl")
	workers := flag.Int("workers", 3, "Number of concurrent scraping workers")
	flag.Parse()

	log.Printf("Starting crawler with %d workers, limit=%d.", *workers, *limit)

	queue := make(chan string, 100)
	visited := make(map[string]bool)
	var mu sync.Mutex

	var workersWg sync.WaitGroup
	var tasksWg sync.WaitGroup
	count := 0

	// Seed the queue dynamically based on Topic
	var startingURLs []string
	if *topic != "" {
		log.Printf("Searching Wikipedia for topic: '%s'...", *topic)
		startingURLs = searchTopicOnWikipedia(*topic)
		if len(startingURLs) == 0 {
			log.Fatalf("Could not find any articles for topic: %s", *topic)
		}
	} else {
		startingURLs = append(startingURLs, *startURL)
	}

	// Start workers
	for w := 1; w <= *workers; w++ {
		workersWg.Add(1)
		go func(workerID int) {
			defer workersWg.Done()
			for target := range queue {
				mu.Lock()
				if count >= *limit {
					mu.Unlock()
					tasksWg.Done()
					continue
				}
				count++
				currentCount := count
				mu.Unlock()

				log.Printf("[W%d] Crawling %d/%d: %s", workerID, currentCount, *limit, target)
				outlinks := crawlPage(target)

				// Enqueue found links if not visited
				mu.Lock()
				for _, link := range outlinks {
					if !visited[link] && count+len(queue) < *limit {
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

	// Wait for all tasks to be gracefully completed or limit reached (using a periodic checker)
	for {
		mu.Lock()
		cnt := count
		mu.Unlock()
		if cnt >= *limit {
			break
		}

		select {
		case <-doneWait:
			break
		case <-time.After(100 * time.Millisecond):
			// continue checking limit in the loop
		}

		// secondary check for donewait to break loop immediately if finished
		select {
		case <-doneWait:
			break
		default:
		}

		if cnt >= *limit {
			break
		}

		// Actually break if doneWait is closed!
		closed := false
		select {
		case <-doneWait:
			closed = true
		default:
		}
		if closed {
			break
		}
	}

	close(queue)
	workersWg.Wait()
	log.Println("Crawler finished gracefully.")
}

// searchTopicOnWikipedia hits the Opensearch API and returns a list of article URLs
func searchTopicOnWikipedia(topic string) []string {
	apiURL := fmt.Sprintf("https://en.wikipedia.org/w/api.php?action=opensearch&search=%s&limit=5&format=json", url.QueryEscape(topic))
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		log.Printf("Failed to create Wikipedia request: %v", err)
		return nil
	}
	req.Header.Set("User-Agent", "SearchSphere Crawler / 1.0 (test@example.com)")

	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		log.Printf("Wikipedia search failed: %v", err)
		return nil
	}
	defer res.Body.Close()

	// Wikipedia Opensearch returns: [ "search term", ["Title1", ...], ["Desc1", ...], ["Url1", ...] ]
	var payload []interface{}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		log.Printf("Failed to decode Wikipedia response: %v", err)
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
		log.Printf("error creating request for %s: %v", target, err)
		return outlinks
	}
	req.Header.Set("User-Agent", "SearchSphere Crawler / 1.0 (test@example.com)")

	res, err := client.Do(req)
	if err != nil {
		log.Printf("error fetching %s: %v", target, err)
		return outlinks
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Printf("skipping %s: HTTP %d", target, res.StatusCode)
		return outlinks
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return outlinks
	}

	title := strings.TrimSpace(doc.Find("title").First().Text())

	// Extract main text from paragraphs
	var sb strings.Builder
	doc.Find("p").Each(func(i int, s *goquery.Selection) {
		text := strings.TrimSpace(s.Text())
		if len(text) > 20 { // skip tiny fragments
			sb.WriteString(text)
			sb.WriteString("\n\n")
		}
	})
	content := strings.TrimSpace(sb.String())

	if title == "" || content == "" {
		return outlinks // skip useless pages
	}

	// Figure out base domain
	parsedURL, _ := url.Parse(target)
	domain := parsedURL.Host

	// Push to search engine
	pushToSearchSphere(target, title, content, domain)

	// Discover new links
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists {
			// Resolve relative URLs
			linkURL, err := url.Parse(href)
			if err == nil {
				absURL := parsedURL.ResolveReference(linkURL)
				absURL.Fragment = "" // normalize anchors
				urlStr := absURL.String()
				// Only crawl HTTP/HTTPS
				if strings.HasPrefix(urlStr, "http") {
					outlinks = append(outlinks, urlStr)
				}
			}
		}
	})

	return outlinks
}

func pushToSearchSphere(sourceURL, title, content, domain string) {
	// MD5 the URL to make a unique ID
	hash := md5.Sum([]byte(sourceURL))
	id := fmt.Sprintf("web-%x", hash)

	reqObj := IngestRequest{
		ID:      id,
		Title:   title,
		Content: content,
		Metadata: map[string]string{
			"url":    sourceURL,
			"domain": domain,
			"source": "crawler",
		},
	}

	body, _ := json.Marshal(reqObj)

	req, err := http.NewRequest("POST", SearchSphereIndexURL, bytes.NewBuffer(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+SearchSphereAPIKey)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("failed to index %s: %v", sourceURL, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		log.Printf("Indexed: %s (ID: %s)", title, id)
	} else {
		log.Printf("Failed to index %s: HTTP %d", sourceURL, resp.StatusCode)
	}
}
