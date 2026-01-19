# System Pipeline: Operational Walkthrough

A detailed step-by-step guide to the "Life of a URL" in the Normalized-Only Architecture.

## 1. The Pipeline: From URL to Verdict

### Step 0: The Input
*   **Source**: Seed URL (e.g., `https://example.com/`) or Task Queue.
*   **Pre-Processing**: **Canonicalization** (Strict Invariant).
    *   `HTTP` -> `HTTPS`
    *   `example.com/` -> `example.com` (Strip slash)
    *   `?utm_source=twitter` -> `(Removed)`
*   **Output**: A clean `Canonical URL`.
*   **Worker**: Scheduler / Task Manager.

### Phase 1: Crawl & Fetch (The Gatherer)
*   **Input**: `Canonical URL`.
*   **Action**:
    *   Connects to internet (Proxy/Tor).
    *   Downloads **Raw HTML Body** into **RAM**.
*   **Blocking**:
    *   Blocks 404s, 500s.
    *   Blocks non-HTML explicitly.
*   **Worker**: `Network IO Worker` (High concurrency).
*   **Time**: **500ms - 2s** (Network constrained).
*   **Output**: `Raw Response Object` (Memory Only). **NEVER PERSISTED.**

### Phase 2: Normalization (The Filter - CRITICAL)
*   **Input**: `Raw Response Object` (Bytes).
*   **Action**:
    *   **Parses**: Bytes -> DOM.
    *   **Strips Noise**: Scripts, styles, ads, dynamic attributes.
    *   **Formats**: Collapses whitespace.
*   **Invariant**: Visual equivalence = Byte equivalence.
*   **Worker**: `CPU Worker`.
*   **Time**: **10ms - 50ms**.
*   **Output**: `PageVersion` (Normalized Text).

### Phase 3: Hashing (The Fingerprint)
*   **Input**: `PageVersion`.
*   **Action**: `SHA-256(NormalizedText)`.
*   **Worker**: `CPU Worker`.
*   **Time**: **< 1ms**.
*   **Output**: `ContentHash`.

### Phase 4: Baseline Management (The Archivist)
*   **Input**: `PageVersion` + `ContentHash`.
*   **Action**:
    *   If **New URL**: Save as `Baseline v1`.
    *   If **Existing**: Retrieve `BaselineHash`.
*   **Worker**: `DB Worker`.
*   **Time**: **5ms - 10ms** (DB Latency).
*   **Output**: `BaselineHash` (for comparison).

### Phase 5: Detection (The Judge)
*   **Input**: `CurrentHash` vs `BaselineHash`.
*   **Action**:
    *   **MATCH** (`Hashes Equal`):
        *   Update `last_seen`.
        *   **DISCARD Content**. (Deduplication).
    *   **MISMATCH** (`Hashes Differ`):
        *   Save **New** `PageVersion` content.
        *   Create `DetectionEvent` linked to `previous_baseline_version_id` and `current_page_version_id`.
*   **Worker**: `Logic Worker`.
*   **Time**: **< 1ms**.
*   **Output**: `Verdict` (Safe/Alert).

---

## 2. Operational Estimates (The "Physics")

### Storage Efficiency
*   **Raw HTML**: ~1MB per crawl (Full DOM).
*   **Normalized**: ~50KB per crawl (Text Only).
*   **Deduplication**: 
    *   99% of crawls change nothing -> **0 Bytes** stored.
    *   **Total Savings**: ~95-99% vs Raw Archival.

### Throughput
*   **Bottleneck**: Phase 1 (Network limit).
*   **Processing Power**: Phases 2-5 take **~50ms total**.
*   **Capacity**: Scales linearly with Fetch Workers. Backend processing is O(1) and extremely cheap.

### Explicit Blocks
*   **Noise**: Tracking params stripped at Step 0.
*   **Junk**: Binary files blocked at Phase 1.
*   **Duplicates**: Identical content discarded at Phase 5.
