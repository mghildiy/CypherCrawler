# CypherLabs Crawler

A **multi-threaded web crawler** built in Java using Virtual Threads and Platform Threads to efficiently fetch and process documents from the web, tokenize and stem content, and build an inverted index.

---

## Features

- Scalable document fetching using **Virtual Threads**
- CPU-bound processing with **Platform Threads**
- Automatic **retry mechanism** for failed fetches
- **Stemming** with Snowball stemmer for normalized indexing
- Thread-safe data handling with `BlockingQueue`, `ConcurrentHashMap`, and `AtomicInteger`
- Generates an **inverted index**: `token → set of URLs`
- Uses **NanoHTTPD** to serve test HTML pages during tests

---

## How It Works

1. **Document Fetchers** (I/O-bound, virtual threads):
    - Take URLs from a crawl frontier queue
    - Fetch HTML using Jsoup
    - Place document-URL pair into a processing queue

2. **Document Processors** (CPU-bound, platform threads):
    - Extract text and links from HTML
    - Tokenize and stem the text
    - Update a global inverted index (`Map<Token, Set<Url>>`)

3. Completion detection:
    - Crawling finishes when:
        - Crawl frontier is empty
        - Processing queue is empty
        - No documents are being fetched or processed

---

## Building Project

```bash
mvn clean install
```

##  Running tests

```bash
mvn test
```