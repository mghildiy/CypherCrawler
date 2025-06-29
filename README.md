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

## Index structure
```
Each index segment is stored as a directory containing the following files:

token_dict.bin
   - Purpose: Stores each token and its byte offset in `postings.bin`
   - **Format**: Each entry is written using `writeUTF(token)` and `writeLong(offset)`
   - **Sorted**: Tokens are written in lexicographic order
   
   Example:
   "apple" → offset 0
   "banana" → offset 17
   "zebra" → offset 45
   
   Three different versions implemented:
   Sorted token dictionary
   - Format: writeUTF(token) + writeLong(offset)
   - Pros: Simple and compact.
   - Read Strategy: Can support binary search (on-disk or in-memory).
   - Use when: Token count is small or fully loaded in memory.
   
   Fixed token width dictionary:
   - Format: fixed-length padded token + writeLong(offset)
   - Pros: Supports direct indexed access (O(1)) with sampled index.
   - Read Strategy: Fast on-disk lookup with seek(i * recordSize).
   - Use when: Token set is large; avoid full in-memory load.
   
   Trie based dictionary:
   - Format: Custom serialized trie of characters and offsets.
   - Pros: Enables efficient prefix matching (e.g., autocompletion), space-sharing via common prefixes.
   - Read Strategy: Tree traversal (either partially in-memory or hybrid memory+disk).
   - Use when: Prefix search or memory-efficient token access is required.

postings.bin
   - Purpose: Stores the list of document IDs for each token
   - Format:
      - First: number of doc IDs (`int`)
      - Then: doc IDs (`int[]`), optionally delta-encoded and varint-compressed
   
   Example:
   For token `"apple"` with doc IDs: `[3, 10, 12]`
   - Delta encoded: `[3, 7, 2]`
   - Varint encoded (hex): `03 07 02`
   
   Binary layout (hex):
   
   03 // number of docIDs
   03 07 02 // delta+varint encoded docIDs
   doc_table.bin
   - Purpose: Stores document metadata (e.g., URL) by doc ID
   - Format:
      - First: total document count (`int`)
      - Then: list of URLs, one per doc ID (`writeUTF(url)`)
   
   Example contents:
   03 // number of docs
   "https://a.com" // doc ID 0
   "https://b.com" // doc ID 1
   "https://c.com" // doc ID 2

bloom.bin
   - Purpose: Serialized Bloom filter of tokens in this segment
   - Usage: Fast exclusion of tokens not present in this segment
   - Format: Raw bytes
   
   Example (binary):
   [256-bit bloom filter]
   00000001 00000000 01000000 ...

segment.meta
   - Purpose: Metadata about this segment (for coordination, merging, stats)
   - Format: JSON (recommended)

   Example json
   {
     "segment_id": "segment_20250623_001",
     "token_count": 12045,
     "doc_count": 1200,
     "created_at": "2025-06-23T10:30:00Z"
   }
```

## Read-side Flow

Given a search token, the engine performs the following steps:

1. **Lookup Token Offset**
    - Use `token_dict.bin` to find the byte offset of the token's posting list in `postings.bin`.
    - Depending on indexing strategy:
        - Use binary search if tokens are sorted.
        - Use fixed-width access if entries are uniformly padded.
        - (Optional) Use a sampled index for faster narrowing down.

2. **Read Postings List**
    - Seek to the offset in `postings.bin`.
    - Read the number of document IDs and then the corresponding list of `docId`s.
    - `postings.bin` can be memory-mapped for efficient random access.

3. **Map `docId`s to URLs**
    - Use `doc_table.bin` to translate each `docId` into its original document URL (or path/title).
    - `doc_table.bin` is typically fully loaded into memory as a list where:
        - `docId == index in the list`, enabling O(1) access.

4. **Return Results**
    - Collate the list of matching URLs (or document metadata) and return it as the search result.

## Read patterns vs Access strategies

| **Read Pattern**                         | **Best Strategy**                        | **Why It Works Well**                                                                | **Examples / Notes**                          |
| ---------------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------ | --------------------------------------------- |
| 🔁 **Sequential Read (start → end)**     | **Buffered Read**                        | Fewer system calls, large block reads → efficient                                    | Logs, WALs, streaming files                   |
| 🎯 **Point Lookup (small dict)**         | **Full In-Memory (Map/List)**            | O(1) or O(logN) access, ultra-fast if it fits in RAM                                 | Token dictionary for small index              |
| 🔍 **Point Lookup (large dict)**         | **Sampled Index over Sorted Disk File**  | Reduces seeks by narrowing scan window, uses binary search on disk                   | Lucene-style index, if RAM is constrained     |
| 🔍 **Point Lookup (large, fixed width)** | **Fixed-Width Records + Seek**           | O(1) offset calc with predictable seeks                                              | Only if token list size and width are bounded |
| 🔤 **Prefix Search / Range Scan**        | **Trie (in-memory or FST)**              | Enables fast prefix/range queries with minimal storage                               | Autocomplete, Lucene's FST                    |
| 📥 **Huge file, random-ish reads**       | **Memory-Mapped File (mmap)**            | OS handles paging, no explicit read → fewer system calls, efficient for sparse reads | Postings list, index segments                 |
| 📂 **Rare access, cold data**            | **On-Disk Binary Search (no full load)** | Saves memory at cost of multiple seeks                                               | Only when full loading isn't an option        |


## TODOs

- STOP words
- Non-english languages
