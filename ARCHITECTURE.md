# System Architecture: Security-Focused Web Crawler

## 1. System Phases (Ordered)
1. **URL Normalization (Shared Contract)**
2. **Frontier & Orchestration Phase**
3. **Crawl Phase**
4. **Baseline Generation Phase**
5. **Defacement Detection Phase**

---

## 2. Phase 1: URL Normalization
- **Responsibilities**:
    - Enforce a single canonical format for all URLs.
    - Resolve relative links to absolute URLs.
    - Remove tracking parameters and fragments that don't change content.
- **Non-Responsibilities**:
    - Validating URL existence (DNS resolution).
    - Managing crawl queues or deduplication state.
- **Data Contract**:
    - **In**: Raw URL String.
    - **Out**: `NormalizedURL` (Canonical String, Domain, Path Components).
- **Failure Semantics**:
    - Malformed strings result in immediate discard; log as `INVALID_CONTRACT`.

---

## 3. Phase 2: Frontier & Orchestration Phase
- **Responsibilities**:
    - Owning the unit of work (URL-level tasks).
    - Managing crawl state transitions (PENDING → IN_PROGRESS → DONE → FAILED).
    - Enforcing deduplication and scoping rules.
    - Assigning work to Crawl Phase workers.
- **Non-Responsibilities**:
    - Performing network requests.
    - Parsing or analyzing content.
    - Baseline or detection logic.
- **Data Contract**:
    - **In**: `NormalizedURL`.
    - **Out**: `CrawlTask` (URL, Depth, Scope, TaskState).
- **Failure Semantics**:
    - Worker crashes MUST return tasks to `PENDING`.
    - No task may remain indefinitely in `IN_PROGRESS`.

---

## 4. Phase 3: Crawl Phase
- **Responsibilities**:
    - Execution of HTTP requests.
    - Handling retries, timeouts, and redirects.
    - Atomic storage of raw response data (HTML/Headers).
- **Non-Responsibilities**:
    - Logic for defacement detection or content analysis.
    - State mutation of existing baselines.
- **Data Contract**:
    - **In**: `CrawlTask`.
    - **Out**: `CrawlArtifact` (Raw Body, Content-Type, HTTP Status, Timestamp).
- **Failure Semantics**:
    - If a request fails after all retries, the task status must be updated via the Frontier to `FAILED`.

---

## 5. Phase 4: Baseline Generation Phase
- **Responsibilities**:
    - Feature extraction from `CrawlArtifact` (e.g., DOM structure, core text).
    - Generating cryptographic or fuzzy signatures for "Ground Truth".
    - Versioning profiles for temporal comparison.
- **Non-Responsibilities**:
    - Direct network access.
    - Comparing current content to previous content.
- **Data Contract**:
    - **In**: `CrawlArtifact`.
    - **Out**: `BaselineProfile` (Extracted Features, Signatures, Metadata).
- **Failure Semantics**:
    - Failure to parse an artifact results in a `CORRUPT_ARTIFACT` state for that URL; does not affect global pipeline.

---

## 6. Phase 5: Defacement Detection Phase
- **Responsibilities**:
    - Orchestrating a comparison between a new `CrawlArtifact` and a `BaselineProfile`.
    - Calculating similarity scores and identifying significant drift.
    - Issuing alerts for high-confidence defacements.
- **Non-Responsibilities**:
    - Modifying baselines or crawl data.
    - Re-fetching data if comparison fails.
- **Data Contract**:
    - **In**: (`CrawlArtifact`, `BaselineProfile`).
    - **Out**: `DetectionVerdict` (Status: [SAFE, DEFACED, ERR], Delta Score, Breach Indicators).
- **Failure Semantics**:
    - If no baseline exists, status must be `NO_BASELINE`.
    - Comparison errors must not flag a site as "Safe"; they must be logged as `ANALYSIS_FAIL`.

---

## 7. Shared Infrastructure
- **Artifact Store**:
    - Written by: Crawl Phase
    - Read by: Baseline & Detection Phases
- **Baseline Store**:
    - Written by: Baseline Generation Phase
    - Read by: Defacement Detection Phase
- **Task Store (Frontier)**:
    - Written by: Frontier & Orchestration Phase
    - Read by: Crawl Phase

---

## 8. System Invariants
1. The unit of work is a single NormalizedURL.
2. A URL may exist in only one task state at a time.
3. No phase may mutate data owned by another phase.
4. Detection output must never modify baselines.

---

## 9. Ambiguities
1. **Storage Layer**: The specific technology for the stores (SQL, NoSQL, or File-based) is not defined.
2. **Scoping**: Whether the crawler supports multi-root domains or is restricted to a single domain is unspecified.
