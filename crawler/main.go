package main

import (
	"bytes"
	"compress/bzip2"
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
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
	limit := flag.Int("limit", 15, "Maximum number of pages to index")
	workers := flag.Int("workers", 3, "Number of concurrent scraping workers")
	dumpFile := flag.String("dump", "", "Path to Wikipedia xml.bz2 dump to ingest directly")
	resume := flag.Bool("resume", false, "Resume dump ingestion from checkpoint.json")
	topicFile := flag.String("topics", "", "Path to a text file containing topics/keywords to filter offline dump")
	flag.Parse()

	if *dumpFile != "" {
		log.Printf("Starting dump ingestor for %s with limit %d", *dumpFile, *limit)
		ingestDump(*dumpFile, *topicFile, *limit, *workers, *resume)
	} else {
		log.Printf("Starting web crawler with %d workers, limit=%d.", *workers, *limit)
		runCrawler(*topic, *startURL, *limit, *workers)
	}
}

// ---------------------------------------------------------
// Dump Ingestion Logic
// ---------------------------------------------------------

// A minimal struct to capture standard Wikipedia page data
type WikiPage struct {
	Title    string    `xml:"title"`
	Ns       int       `xml:"ns"`
	Redirect *struct{} `xml:"redirect"`
	Revision struct {
		Text string `xml:"text"`
	} `xml:"revision"`
}

func ingestDump(filepath string, topicFile string, limit int, workers int, resume bool) {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Failed to open dump file: %v", err)
	}
	defer file.Close()

	var filters []string
	var filterPatterns []*regexp.Regexp
	categoryRegex := regexp.MustCompile(`(?i)\[\[Category:([^\]]+)\]\]`)

	if topicFile != "" {
		if b, err := os.ReadFile(topicFile); err == nil {
			lines := strings.Split(string(b), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" {
					filters = append(filters, strings.ToLower(line))
					filterPatterns = append(filterPatterns, regexp.MustCompile(`(?i)\b`+regexp.QuoteMeta(line)+`\b`))
				}
			}
			log.Printf("Loaded %d keyword filters from %s (Relevance Scoring Active)", len(filters), topicFile)
		} else {
			log.Printf("Failed to read topics file %s: %v", topicFile, err)
		}
	}

	bz2Reader := bzip2.NewReader(file)
	decoder := xml.NewDecoder(bz2Reader)

	count := 0
	skipCount := 0

	if resume {
		if b, err := os.ReadFile("checkpoint.json"); err == nil {
			var state struct {
				Count int `json:"count"`
			}
			if json.Unmarshal(b, &state) == nil && state.Count > 0 {
				skipCount = state.Count
				log.Printf("Resuming dump from checkpoint.json, fast-forwarding %d articles...", skipCount)
			}
		}
	}

	// Create a worker pool for fast pushing
	var wg sync.WaitGroup
	pageChan := make(chan WikiPage, 100)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var batch []IngestRequest
			for p := range pageChan {
				// Generate Wikipedia URL and ID
				sourceURL := fmt.Sprintf("https://en.wikipedia.org/wiki/%s", url.PathEscape(strings.ReplaceAll(p.Title, " ", "_")))
				hash := md5.Sum([]byte(sourceURL))
				id := fmt.Sprintf("web-%x", hash)

				batch = append(batch, IngestRequest{
					ID:      id,
					Title:   p.Title,
					Content: p.Revision.Text,
					Metadata: map[string]string{
						"url":    sourceURL,
						"domain": "en.wikipedia.org",
						"source": "crawler",
					},
				})

				if len(batch) >= 50 {
					pushBulkToSearchSphere(batch)
					batch = nil
				}
			}
			// push any remaining
			if len(batch) > 0 {
				pushBulkToSearchSphere(batch)
			}
		}()
	}

	for {
		if count >= limit {
			break
		}

		t, err := decoder.Token()
		if err != nil {
			if err.Error() == "EOF" {
				break // End of document
			}
			log.Printf("XML parsing error: %v", err)
			break
		}

		switch se := t.(type) {
		case xml.StartElement:
			if se.Name.Local == "page" {
				var page WikiPage
				if err := decoder.DecodeElement(&page, &se); err != nil {
					log.Printf("Error decoding page: %v", err)
					continue
				}

				// Only consider main namespace (0), ignore templates, talk pages, etc.
				// Also ignore redirect pages
				text := strings.TrimSpace(page.Revision.Text)
				if page.Ns == 0 && page.Redirect == nil && len(text) > 50 && !strings.HasPrefix(strings.ToLower(text), "#redirect") {

					cleanedText := cleanWikiMarkup(text)
					if len(cleanedText) > 50 {

						if len(filters) > 0 {
							// Extract raw categories before they were stripped
							rawCategories := categoryRegex.FindAllStringSubmatch(text, -1)
							var matchedCategories string
							for _, cat := range rawCategories {
								if len(cat) > 1 {
									matchedCategories += " " + strings.ToLower(cat[1])
								}
							}

							lowerTitle := strings.ToLower(page.Title)

							// Compute relevance score strictly weighted toward intent
							highestTopicScore := 0
							for _, pattern := range filterPatterns {
								topicScore := 0

								// 1. Strict Regex Title match (+20 points)
								if pattern.MatchString(lowerTitle) {
									topicScore += 20
								}

								// 2. Strict Regex Wikipedia Category match (+20 points)
								if pattern.MatchString(matchedCategories) {
									topicScore += 20
								}

								// 3. Body occurrence frequency (+1 point each, capped at 10)
								matches := len(pattern.FindAllStringIndex(cleanedText, -1))
								if matches > 10 {
									matches = 10
								}
								topicScore += matches

								if topicScore > highestTopicScore {
									highestTopicScore = topicScore
								}
							}

							// Aggressive threshold: must achieve 20 points in a SINGLE intended topic
							if highestTopicScore < 20 {
								continue
							}
						}
						if skipCount > 0 {
							skipCount--
							count++  // increment raw absolute count
							continue // purely skip this valid article
						}

						page.Revision.Text = cleanedText
						count++
						log.Printf("Queuing page %d/%d: %s", count, limit, page.Title)
						pageChan <- page

						// Periodically write checkpoint
						if count%100 == 0 {
							state := struct {
								Count int `json:"count"`
							}{Count: count}
							b, _ := json.Marshal(state)
							os.WriteFile("checkpoint.json", b, 0644)
						}
					}
				}
			}
		}
	}

	close(pageChan)
	wg.Wait()
	log.Println("Dump ingestion finished successfully.")
}

// cleanWikiMarkup strips MediaWiki specific code from the text.
func cleanWikiMarkup(text string) string {
	// A simple state machine to remove {{...}}
	var sb strings.Builder
	inTemplate := 0

	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		if i+1 < len(runes) && runes[i] == '{' && runes[i+1] == '{' {
			inTemplate++
			i++
			continue
		}
		if i+1 < len(runes) && runes[i] == '}' && runes[i+1] == '}' {
			if inTemplate > 0 {
				inTemplate--
			}
			i++
			continue
		}
		if inTemplate > 0 {
			continue
		}
		sb.WriteRune(runes[i])
	}

	res := sb.String()

	// Remove HTML comments
	res = regexp.MustCompile(`(?s)<!--.*?-->`).ReplaceAllString(res, "")

	// Remove references like <ref ...>...</ref> perfectly via regex non-greedy
	res = regexp.MustCompile(`(?is)<ref[^>]*>.*?</ref>`).ReplaceAllString(res, "")
	res = regexp.MustCompile(`(?is)<ref[^>]*/>`).ReplaceAllString(res, "")

	// Remove [[File:...]] or [[Image:...]] or [[Category:...]]
	res = regexp.MustCompile(`(?i)\[\[(?:File|Image|Category):.*?\]\]`).ReplaceAllString(res, "")

	// Fix piped links [[Link|Text]] -> Text
	res = regexp.MustCompile(`\[\[[^\]]+\|([^\]]+)\]\]`).ReplaceAllString(res, "$1")

	// Fix simple links [[Link]] -> Link
	res = strings.ReplaceAll(res, "[[", "")
	res = strings.ReplaceAll(res, "]]", "")

	// Remove bold/italic
	res = strings.ReplaceAll(res, "'''", "")
	res = strings.ReplaceAll(res, "''", "")

	// Standardize headers == Heading ==
	res = regexp.MustCompile(`==+\s*([^=]+)\s*==+`).ReplaceAllString(res, "$1\n")

	// Collapse multiple spaces
	res = regexp.MustCompile(`\n{3,}`).ReplaceAllString(res, "\n\n")

	return strings.TrimSpace(res)
}

// ---------------------------------------------------------
// Live Web Crawler Logic
// ---------------------------------------------------------

func runCrawler(topic string, startURL string, limit int, workers int) {
	queue := make(chan string, 100)
	visited := make(map[string]bool)
	var mu sync.Mutex

	var workersWg sync.WaitGroup
	var tasksWg sync.WaitGroup
	count := 0

	var startingURLs []string
	if topic != "" {
		log.Printf("Searching Wikipedia for topic: '%s'...", topic)
		startingURLs = searchTopicOnWikipedia(topic)
		if len(startingURLs) == 0 {
			log.Fatalf("Could not find any articles for topic: %s", topic)
		}
	} else {
		startingURLs = append(startingURLs, startURL)
	}

	for w := 1; w <= workers; w++ {
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

				log.Printf("[W%d] Crawling %d/%d: %s", workerID, currentCount, limit, target)
				outlinks := crawlPage(target)

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

Loop:
	for {
		mu.Lock()
		cnt := count
		mu.Unlock()
		if cnt >= limit {
			break Loop
		}

		select {
		case <-doneWait:
			break Loop
		case <-time.After(100 * time.Millisecond):
		}
	}

	close(queue)
	workersWg.Wait()
	log.Println("Crawler finished gracefully.")
}

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

	var sb strings.Builder
	doc.Find("p").Each(func(i int, s *goquery.Selection) {
		text := strings.TrimSpace(s.Text())
		lowerText := strings.ToLower(text)

		// Filter out common SPA loading/error fallbacks
		if strings.Contains(lowerText, "error while loading") ||
			strings.Contains(lowerText, "please reload this page") ||
			strings.Contains(lowerText, "enable javascript") ||
			strings.Contains(lowerText, "javascript is disabled") {
			return
		}

		if len(text) > 20 {
			sb.WriteString(text)
			sb.WriteString("\n\n")
		}
	})
	content := strings.TrimSpace(sb.String())

	if title == "" || content == "" {
		return outlinks
	}

	parsedURL, _ := url.Parse(target)
	domain := parsedURL.Host

	pushToSearchSphere(target, title, content, domain)

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

func pushToSearchSphere(sourceURL, title, content, domain string) {
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

func pushBulkToSearchSphere(docs []IngestRequest) {
	if len(docs) == 0 {
		return
	}
	body, _ := json.Marshal(map[string]interface{}{
		"documents": docs,
	})

	req, err := http.NewRequest("POST", "http://localhost:8080/bulk", bytes.NewBuffer(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+SearchSphereAPIKey)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("failed to bulk index %d docs: %v", len(docs), err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		log.Printf("Bulk Indexed %d documents successfully", len(docs))
	} else {
		log.Printf("Failed to bulk index: HTTP %d", resp.StatusCode)
	}
}
