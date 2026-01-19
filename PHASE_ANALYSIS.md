# **WEB CRAWLER PIPELINE - 7 PHASE BREAKDOWN**

---

## **PHASE 1: INITIATION**

### **INPUT:**
- Environment variables (`CRAWL_MODE` from `.env`)
- MySQL database connection parameters
- Customer & Site records from `sites` table
  - Each site has: `siteid`, `custid`, `url`, `enabled` flag
  - The `url` column is the actual seed URL (no separate raw URL needed)

### **STEPS:**
1. **Environment Setup** - Load `.env`, validate `CRAWL_MODE` ∈ {BASELINE, CRAWL, COMPARE}
2. **Database Health Check** - `check_db_health()` validates MySQL connectivity
3. **Site Discovery** - `fetch_enabled_sites()` queries all enabled sites (url is already the seed)
4. **URL Normalization** - `normalize_url()` removes trailing slashes (except root), preserves query/fragments
5. **Job Registration** - Generate UUID, call `insert_crawl_job()` to create MySQL record with status='running'
6. **Frontier Initialization** - Create thread-safe queue, visited/in-progress sets
7. **Worker Spawn** - Launch worker threads with frontier reference, custid, siteid, job_id, crawl_mode, domain_list

### **ROLE OF WORKER:**
None - Workers are idle during initialization; main thread handles all setup.

### **TIME TAKEN:**
**~2-5 seconds per site**
- DB health check: <100ms
- Site query: <50ms  
- Seed resolution: 1-3s (DNS + HTTP roundtrip)
- Worker spawn: <200ms

### **OUTPUT:**
- **Console Prints:**
  ```
  MySQL health check passed.
  Found 1 enabled site(s).
  ============================================================
  Starting crawl job a1b2c3d4-...
  Customer ID : 101
  Site ID     : 5001
  Seed URL    : https://example.com
  ============================================================
  Started 5 workers.
  ```
- **Database Record:** New row in `crawl_jobs` table (job_id, custid, siteid, status='running', started_at)
- **Memory State:** Frontier queue seeded with start_url from sites table

### **FILES & FUNCTIONS:**
- [main.py](baseline-crawler/main.py) - `main()`, `resolve_seed_url()`
- [crawler/storage/db.py](baseline-crawler/crawler/storage/db.py) - `check_db_health()`, `fetch_enabled_sites()`, `insert_crawl_job()`
- [crawler/frontier.py](baseline-crawler/crawler/frontier.py) - `Frontier.__init__()`, `enqueue()`
- [crawler/normalizer.py](baseline-crawler/crawler/normalizer.py) - `normalize_url()`
- [crawler/worker.py](baseline-crawler/crawler/worker.py) - `Worker.__init__()`

---

## **PHASE 2: CRAWL & FETCH**

### **INPUT:**
- URL from frontier queue: `(url, parent_url)`
- Domain whitelist from `sites` table (normalized domains only)
- Configuration: `REQUEST_TIMEOUT=10s`, `USER_AGENT`

### **STEPS:**
1. **Queue Dequeue** - Worker calls `frontier.dequeue()`, blocks if empty
2. **Domain Validation** - `_is_allowed_domain()` checks against domain list from `sites` table (exact domain matching, no www variants)
3. **Block Rule Check** - `classify_block()` rejects tag/author/pagination pages, static assets
4. **HTTP Request** - `fetch()` issues `requests.get()` with timeout, User-Agent, SSL verification
5. **Response Classification** - Check status code (2xx=success), Content-Type (HTML/JSON only)
6. **Error Handling** - Catch timeout, connection errors, request exceptions
7. **Database Recording** - `insert_crawl_page()` writes job_id, custid, siteid, url, parent, status_code, content_type, size, response_time_ms, fetched_at

### **ROLE OF WORKER:**
**Primary executor** - Each worker thread independently:
- Dequeues URLs from shared frontier
- Performs HTTP fetch
- Records metadata to database
- Passes HTML to next phase
- Marks URL as visited in frontier

### **Worker Synchronization (Duplicate URL Prevention):**

**Case 1: Sequential Same URL Fetch**
```
Time 1: Worker-0 dequeues https://example.com/page
Time 2: Worker-0 marks URL in visited set (atomic operation with lock)
Time 3: Worker-1 calls enqueue(https://example.com/page)
        → frontier.enqueue() checks visited set (locked)
        → URL already in visited, returns False
        → URL NOT enqueued, prevents duplicate fetch
```

**Case 2: Concurrent Same URL Fetch**
```
Time 1: Worker-0 dequeues https://example.com/page
        → Moves URL from queue to in_progress set (atomic with lock)
Time 1: Worker-1 calls enqueue(https://example.com/page)
        → frontier.enqueue() checks in_progress set (locked)
        → URL already in_progress, returns False
        → URL NOT enqueued, prevents concurrent fetch
        
Time 2: Worker-0 completes fetch, calls mark_visited()
        → Moves URL from in_progress to visited set
        → Thread-safe lock ensures no race condition
```

**Synchronization Mechanism:**
- **Lock**: `Frontier.lock` protects all state changes
- **Visited Set**: Tracks permanently crawled URLs
- **In-Progress Set**: Tracks URLs currently being fetched
- **Dequeue Atomicity**: When URL dequeued, immediately moved to in_progress under lock

### **TIME TAKEN:**
**~200ms - 3s per URL**
- DNS lookup: 20-100ms (cached after first)
- TCP handshake: 50-200ms
- TLS negotiation: 100-500ms (HTTPS)
- Server response: 100ms-2s (depends on server)
- Network transfer: 50-500ms (depends on HTML size)

### **OUTPUT:**
- **Console Prints:**
  ```
  [Worker-0] started (BASELINE)
  [Worker-0] Crawling https://example.com
  [Worker-1] Crawling https://example.com/about
  ```
- **Database Record:** Row in `crawl_pages` table per successful fetch
- **Memory:** Response object with HTML text passed to parsing phase
- **Block Report:** URLs rejected by domain/block rules added to `BLOCK_REPORT` dict

### **FILES & FUNCTIONS:**
- [crawler/worker.py](baseline-crawler/crawler/worker.py) - `Worker.run()`, `_is_allowed_domain()`, `classify_block()`
- [crawler/fetcher.py](baseline-crawler/crawler/fetcher.py) - `fetch()`
- [crawler/frontier.py](baseline-crawler/crawler/frontier.py) - `dequeue()`, `mark_visited()`
- [crawler/storage/db.py](baseline-crawler/crawler/storage/db.py) - `insert_crawl_page()`

---

## **PHASE 3: NORMALIZATION**

### **INPUT:**
- Raw HTML response text
- Current URL (for resolving relative links)
- Content-Type header

### **STEPS:**
1. **JS Detection** - `needs_js_rendering()` scans for React/Vue/Angular/SPA patterns
2. **JS Rendering (if needed)** - `render_js_sync()` launches headless Chrome, waits for DOM load
3. **Render Caching** - `get_cached_render()` checks Redis/memory cache to avoid re-rendering
4. **HTML Cleanup** - `normalize_rendered_html()` strips trivial comments (LiteSpeed Cache timestamps)
5. **Semantic Normalization** - `semantic_normalize_html()` collapses whitespace, normalizes punctuation, sorts comma-separated lists
6. **Link Extraction** - `extract_urls()` parses `<a>`, `<img>`, `<link>`, `<script>` tags, resolves to absolute URLs, filters to same domain
7. **URL Classification** - `classify_url()` tags each URL as pagination/assets/scripts/API/normal_html

### **Why URL Classification if Block Rules Exist?**

**Block Rules** (`classify_block()`) → **BINARY DECISION (Block/Allow)**
- These rules are binary: STATIC, TAG_PAGE, AUTHOR_PAGE, PAGINATION → **DO NOT ENQUEUE**
- Applied at the frontier level (Phase 2)
- Purpose: Prevent specific patterns from being added to the crawl queue

**URL Classification** (`classify_url()`) → **METADATA TAGGING (For Analysis)**
- Tags URLs with multiple categories: `pagination`, `assets_uploads`, `scripts_styles`, `api_like`, `normal_html`
- Applied during normalization phase (Phase 3) for **informational purposes only**
- Purpose: Provide insights into crawled URL types for:
  - Analytics/reporting
  - Baseline comparison (identify if only pagination changed)
  - Defacement pattern detection (e.g., script injection in normal_html vs api_like)
  - Forensic analysis of which URL types were targeted

**Example:**
```
URL: https://example.com/uploads/image.jpg
classify_block() → "STATIC" → NOT enqueued (blocked in Phase 2)

URL: https://example.com/about
classify_url() → "normal_html" → Enqueued, classified for analytics (Phase 3)

URL: https://example.com/?page=2
classify_url() → "pagination" → Enqueued, tagged as pagination page
                              (can be excluded from baseline if only pagination URL changed)
```

### **ROLE OF WORKER:**
**Data transformer** - Worker thread:
- Detects if JS rendering needed
- Normalizes HTML for stable hashing
- Extracts child URLs for enqueuing
- Does NOT write to disk yet (happens in next phase)

### **TIME TAKEN:**
**~50ms - 2s per URL**
- Static HTML normalization: 50-200ms (BeautifulSoup parsing)
- JS rendering: 1-2s per page (Chrome launch + page load)
- Link extraction: 10-50ms (depends on link count)
- URL classification: <5ms per URL

### **OUTPUT:**
- **Memory:** 
  - Normalized HTML string (stripped comments, collapsed whitespace)
  - List of extracted child URLs
  - Classification dict per URL
- **Render Cache:** JS-rendered HTML stored with SHA256(url) key
- **No Console Prints** (silent transformation phase)

### **FILES & FUNCTIONS:**
- [crawler/normalizer.py](baseline-crawler/crawler/normalizer.py) - `normalize_rendered_html()`, `semantic_normalize_html()`, `strip_trivial_comments()`
- [crawler/parser.py](baseline-crawler/crawler/parser.py) - `extract_urls()`, `classify_url()`
- [crawler/js_detect.py](baseline-crawler/crawler/js_detect.py) - `needs_js_rendering()`
- [crawler/js_renderer.py](baseline-crawler/crawler/js_renderer.py) - `render_js_sync()`
- [crawler/render_cache.py](baseline-crawler/crawler/render_cache.py) - `get_cached_render()`, `set_cached_render()`

---

## **PHASE 4: HASHING**

### **INPUT:**
- Normalized HTML string (from Phase 3)
- URL (for hash-to-URL mapping)

### **STEPS:**
1. **Content Hashing** - `hash_content()` computes SHA256 of normalized HTML ([hasher.py#L6-L10](baseline-crawler/crawler/hasher.py#L6-L10))
2. **Encoding** - Convert string to UTF-8 bytes before hashing
3. **Hexdigest** - Return 64-character hex hash string
4. **DOM Fingerprinting** - `dom_structure_fingerprint()` generates tuple of tag paths (e.g., `/html/body/div/p`) for structural comparison ([normalizer.py#L106-L123](baseline-crawler/crawler/normalizer.py#L106-L123))

### **ROLE OF WORKER:**
**Hash generator** - Worker computes hash during:
- **BASELINE mode:** Before storing snapshot
- **COMPARE mode:** To compare against stored baseline hash

### **TIME TAKEN:**
**~5-20ms per URL**
- SHA256 computation: 5-15ms (depends on HTML size)
- DOM traversal: 5-10ms
- No disk I/O in this phase

### **OUTPUT:**
- **Memory:** 
  - `html_hash` (64-char hex string, e.g., `09d5d200dacb93ad...`)
  - `dom_fingerprint` (sorted tuple of tag paths)
- **No Console Prints**
- **No Database Writes** (hash stored in next phase)

### **FILES & FUNCTIONS:**
- [crawler/hasher.py](baseline-crawler/crawler/hasher.py) - `hash_content()`, `hash_json_keys()`
- [crawler/normalizer.py](baseline-crawler/crawler/normalizer.py) - `dom_structure_fingerprint()`
- [compare_utils.py](baseline-crawler/compare_utils.py) - Baseline hash comparison logic

---

## **PHASE 5: BASELINE MANAGEMENT**

### **INPUT:**
- Normalized HTML (from Phase 3)
- HTML hash (from Phase 4)
- URL, custid, siteid, crawl_mode
- Snapshot file path

### **STEPS:**
1. **Mode Check** - Only executes if `CRAWL_MODE == "BASELINE"` ([worker.py#L155-L168](baseline-crawler/crawler/worker.py#L155-L168))
2. **Directory Structure Creation** - `_get_or_create_site_folder()` creates hierarchy: `baselines/{custid}/{site_folder_id}/` ([baseline_store.py#L16-L37](baseline-crawler/crawler/storage/baseline_store.py#L16-L37))
3. **Filename Generation** - `_get_next_page_filename()` assigns sequential names: `{custid}01.html`, `{custid}01-1.html`, etc. ([baseline_store.py#L40-L57](baseline-crawler/crawler/storage/baseline_store.py#L40-L57))
4. **HTML Canonicalization** - `_canonicalize_for_storage()` converts escaped newlines, strips whitespace ([baseline_store.py#L60-L67](baseline-crawler/crawler/storage/baseline_store.py#L60-L67))
5. **Snapshot Write** - `store_snapshot_file()` writes raw HTML to disk with UTF-8 encoding ([baseline_store.py#L70-L95](baseline-crawler/crawler/storage/baseline_store.py#L70-L95))
6. **Hash Storage** - `store_baseline_hash()` saves URL→hash mapping to database (function referenced but not shown in codebase)
7. **Index Update** - Update `index.json` with domain→site_folder_id mapping and page counter

### **ROLE OF WORKER:**
**Baseline recorder** - In BASELINE mode, worker:
- Writes normalized HTML to hierarchical file structure
- Records hash for future comparisons
- Maintains index files for site organization

### **TIME TAKEN:**
**~20-100ms per URL**
- Directory creation: <5ms (cached after first)
- File write: 10-50ms (depends on HTML size, typically 10-100KB)
- Index update: 5-20ms (JSON read/write with lock)
- Database hash insert: 10-30ms

### **OUTPUT:**
- **File System:**
  ```
  baselines/
    101/                        # custid
      index.json               # domain→site_folder_id mapping
      10101/                   # site_folder_id
        index.json            # page counter
        10101.html            # first page
        10101-1.html          # second page
        10101-2.html          # third page
  ```
- **Database:** Hash record in `baselines` table (normalized_url, html_hash, baseline_path)
- **Console Prints:**
  ```
  [Worker-0] Stored baseline for https://example.com
  ```

### **FILES & FUNCTIONS:**
- [crawler/storage/baseline_store.py](baseline-crawler/crawler/storage/baseline_store.py) - `store_snapshot_file()`, `_get_or_create_site_folder()`, `_get_next_page_filename()`, `load_all_baseline_pages()`
- [crawler/worker.py](baseline-crawler/crawler/worker.py) - BASELINE mode logic in `run()` method
- [crawler/baseline_utils.py](baseline-crawler/crawler/baseline_utils.py) - `safe_baseline_filename()`

---

## **PHASE 6: DETECTION**

### **INPUT:**
- Current page HTML hash (from Phase 4)
- Baseline hash from database (stored in Phase 5)
- URL
- Script sources extracted from `<script src="...">` tags

### **STEPS:**
1. **Mode Check** - Only executes if `CRAWL_MODE == "COMPARE"` ([worker.py#L170-L176](baseline-crawler/crawler/worker.py#L170-L176))
2. **Baseline Lookup** - `get_baseline()` queries database for stored baseline hash by URL
3. **Hash Comparison** - Compare `baseline_hash` vs `observed_hash` ([detector.py#L37-L45](baseline-crawler/crawler/detection/detector.py#L37-L45))
4. **Script Source Check** - Detect added/removed external scripts (HIGH severity) ([detector.py#L47-L65](baseline-crawler/crawler/detection/detector.py#L47-L65))
5. **DOM Structure Check** - Compare DOM fingerprints for node additions/removals (MEDIUM severity) ([detector.py#L98-L108](baseline-crawler/crawler/detection/detector.py#L98-L108))
6. **Semantic Text Check** - `_token_multiset()` compares word frequency (LOW severity if only reordering) ([detector.py#L111-L123](baseline-crawler/crawler/detection/detector.py#L111-L123))
7. **Diff Generation** - `generate_html_diff()` creates .patch and .html diff artifacts ([compare_utils.py#L52-L98](baseline-crawler/compare_utils.py#L52-L98))
8. **Evidence Storage** - `insert_diff()` writes detection record to `diff_evidence` table with severity, hashes, timestamps

### **ROLE OF WORKER:**
**Defacement detector** - In COMPARE mode, worker:
- Calls `compare_engine.handle_page()` for each crawled page
- Compares against stored baselines
- Generates forensic diffs on mismatch
- Does NOT block crawl on detection (continues to next URL)

### **TIME TAKEN:**
**~30-200ms per URL**
- Baseline lookup: 10-30ms (database query)
- Hash comparison: <1ms
- Script source diff: 5-10ms
- DOM fingerprint comparison: 10-30ms
- Semantic text diff: 20-100ms (depends on HTML size)
- Diff generation: 50-150ms (only on mismatch)

### **OUTPUT:**
- **Database Record:** Row in `diff_evidence` table:
  ```sql
  (url, baseline_hash, observed_hash, diff_summary, severity, detected_at, status='open')
  ```
- **File System (on mismatch):**
  ```
  observed/
    101/
      10101/
        10101.html              # observed snapshot
  diffs/
    {url_hash}/
      diff.patch               # unified diff
      diff.html                # side-by-side HTML
      diff.meta.json           # metadata
  ```
- **Console Prints:**
  ```
  [Worker-2] DEFACEMENT DETECTED: https://example.com/page
  [Worker-2] Severity: HIGH - External script added
  [Worker-2] Baseline: 09d5d200... | Observed: 37b26b9b...
  ```
- **Return Value:** Detection result dict:
  ```python
  {
    "defaced": True,
    "severity": "HIGH",
    "severity_reason": "External script source added",
    "html_changed": True,
    "scripts_added": ["https://evil.com/malware.js"],
    "scripts_removed": [],
    "baseline_hash": "09d5d200...",
    "observed_hash": "37b26b9b..."
  }
  ```

### **FILES & FUNCTIONS:**
- [crawler/detection/detector.py](baseline-crawler/crawler/detection/detector.py) - `detect_defacement()`, `_token_multiset()`, `_read_baseline_snapshot()`
- [compare_utils.py](baseline-crawler/compare_utils.py) - `generate_html_diff()`, `load_baseline_snapshot()`
- [crawler/worker.py](baseline-crawler/crawler/worker.py) - COMPARE mode logic calling `compare_engine.handle_page()`
- [crawler/storage/db.py](baseline-crawler/crawler/storage/db.py) - `insert_diff()`

---

## **PHASE 7: UI**

### **INPUT:**
- MySQL database: `crawlerdb` (crawl_jobs, crawl_pages, baselines, diff_evidence tables)
- Snapshot files: `baselines/`, `observed/` directories

### **STEPS:**
1. **Flask App Launch** - Start web server on `http://localhost:5000`
2. **Dashboard Route** - `/` displays summary stats (total jobs, completed jobs, failed jobs, pages crawled, baselines, open alerts)
3. **Failed URLs Route** - `/failed-urls` shows all failed page fetches with status codes and timestamps
4. **Baselines Route** - `/baselines` lists all stored baseline snapshots with URLs, hashes, creation dates
5. **Detections Route** - `/detections` shows all open defacement alerts with severity levels
6. **Detection Detail Route** - `/detection/<id>` loads full defacement information with diff_summary JSON
7. **API Endpoints** - `/api/stats` provides JSON stats for programmatic access

### **ROLE OF WORKER:**
**None** - UI is post-crawl analysis; workers have completed execution.

### **TIME TAKEN:**
**Real-time (user-initiated)**
- Page load: 50-200ms (database queries + template rendering)
- Detection detail: 100-300ms (loads JSON diff summary)
- API endpoints: 30-100ms (cached queries)

### **OUTPUT:**
- **Web Interface:**
  - **Dashboard:** Summary cards showing crawl stats, recent jobs list
  - **Failed URLs Page:** Table of failed fetches with status codes, response times
  - **Baselines Page:** Table of stored baselines with truncated hashes, timestamps
  - **Detections Page:** Table of open alerts with severity badges, quick detail links
  - **Detection Detail:** Full defacement info with JSON diff summary

- **Console Prints (on startup):**
  ```
  * Running on http://127.0.0.1:5000
  * Debug mode: on
  ```

### **FILES & FUNCTIONS:**
- [app.py](baseline-crawler/app.py) - Main Flask app with routes
- [templates/base.html](baseline-crawler/templates/base.html) - Base template with navigation
- [templates/dashboard.html](baseline-crawler/templates/dashboard.html) - Dashboard page
- [templates/failed_urls.html](baseline-crawler/templates/failed_urls.html) - Failed URLs page
- [templates/baselines.html](baseline-crawler/templates/baselines.html) - Baselines page
- [templates/detections.html](baseline-crawler/templates/detections.html) - Detections listing
- [templates/detection_detail.html](baseline-crawler/templates/detection_detail.html) - Detection detail page

---

## **TERMINAL PRINTS CONSOLIDATED**

### **Initiation Phase:**
```
MySQL health check passed.
Found 3 enabled site(s).

============================================================
Starting crawl job f8e9d2a1-c456-4789-b123-456789abcdef
Customer ID : 101
Site ID     : 5001
Seed URL    : https://worldpeoplesolutions.com
============================================================
Started 5 workers.
```

### **Crawl & Fetch Phase:**
```
[Worker-0] started (BASELINE)
[Worker-1] started (BASELINE)
[Worker-2] started (BASELINE)
[Worker-0] Crawling https://worldpeoplesolutions.com
[Worker-1] Crawling https://worldpeoplesolutions.com/about
[Worker-2] Crawling https://worldpeoplesolutions.com/services
[Worker-0] enqueued: https://worldpeoplesolutions.com/contact
[Worker-1] enqueued: https://worldpeoplesolutions.com/blog
```

### **Detection Phase (COMPARE mode only):**
```
[Worker-3] DEFACEMENT DETECTED: https://example.com/compromised
[Worker-3] Severity: HIGH - External script added
[Worker-3] Scripts added: ['https://malicious.site/inject.js']
[Worker-3] Baseline: 09d5d200dacb... | Observed: 37b26b9beb0f...
```

### **Completion Phase:**
```
------------------------------------------------------------
CRAWL COMPLETED
------------------------------------------------------------
Job ID            : f8e9d2a1-c456-4789-b123-456789abcdef
Customer ID       : 101
Site ID           : 5001
Seed URL          : https://worldpeoplesolutions.com
Total URLs visited: 47
Crawl duration    : 23.45 seconds
Workers used      : 5
------------------------------------------------------------

All site crawls completed successfully.

============================================================
BLOCKED URL REPORT
============================================================
[BLOCK_RULE] 12 URLs blocked
[DOMAIN_FILTER] 8 URLs blocked
============================================================
```

### **Error Handling Prints:**
```
[Worker-0] ERROR https://example.com/broken: timeout
[Worker-2] fetch failed for https://example.com/404: http error: 404
ERROR: Crawl job {job_id} failed: Connection refused
```

---

**END OF PHASE ANALYSIS**
