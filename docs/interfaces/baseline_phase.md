# ARCHITECTURE LOCKED — Baseline Generation Phase
# Changes require architecture review.

# Phase 4: Baseline Generation Phase Interface

### 1. Input Schema (Consumed from `CrawlArtifact`)
| Field | Type | Requirement | Description |
| :--- | :--- | :--- | :--- |
| `normalized_url` | String | Required | The unique task identity. |
| `raw_body` | Bytes | Required | The raw content to be processed. |
| `http_status` | Integer | Required | Standard HTTP status code. |
| `content_type` | String | Required | Used to select the extraction strategy. |
| `request_timestamp` | Timestamp | Required | Links the profile to a specific temporal event. |

### 2. Output Schema: `BaselineProfile`
| Field | Type | Description |
| :--- | :--- | :--- |
| `normalized_url` | String | **Primary Link**. Identity of the site. |
| `baseline_id` | UUID/String | Unique instance ID for this specific profile. |
| `structural_digest` | String | Cryptographic hash of the skeleton/DOM structure (stripped of text). |
| `structural_features` | Map<String, Int> | Quantitative tag counts used for continuous drift analysis. |
| `content_features` | Map<String, Any> | Aggregated features (title, meta-tags, anchor text density, etc.). |
| `extraction_version` | String | Versioning string for the logic used during extraction. |
| `created_at` | Timestamp | Duration-stable creation time of the profile. |

### 3. Baseline Selection & Versioning
- **Active Selection Rule**: At most one ACTIVE `BaselineProfile` exists per (`normalized_url`, `extraction_version`).
- **Detection Matching**: The Detection Phase MUST use the most recent `BaselineProfile` matching the current system-wide `extraction_version`.
- **Immutability**: Once written, profiles are never modified. Re-baselining creates a new record.

### 4. Feature Extraction Responsibilities
- **Structural Distillation**: Identifying unstable or dynamic elements (e.g. timestamps, randomized IDs) and excluding them from the `structural_digest`.
- **Normalization**: Enforcing deterministic character encoding (UTF-8) and tag formats before digest calculation.
- **Deduplication**: Ensuring that identical inputs produce identical digests.

### 5. Invariants
- **Baseline Eligibility**: A `BaselineProfile` may ONLY be generated when `200 ≤ http_status < 300`. 
- **Determinism**: Given the same `CrawlArtifact` and `extraction_version`, the resulting `BaselineProfile` must be byte-for-byte identical.
- **Isolated Processing**: Logic must only operate on the provided artifact; no historical lookups allowed during extraction.

### 6. Failure Signaling
- **Structured Outcomes**: On failure, the phase emits NO `BaselineProfile` and returns a failure status:
    - `PROCESS_FAILED`: Data format violation or parsing crash.
    - `EMPTY_CONTENT`: `raw_body` has 0 length.
    - `INELIGIBLE_STATUS`: `http_status` is outside the 200–299 range.

### 7. Forbidden Behaviors
- **No Network I/O**: No DNS, no HTTP, no external dependencies.
- **No State Mutation**: Must not update `Task Store`, `Artifact Store`, or legacy baselines.
- **No Comparison**: Must not perform similarity analysis or alerting (Reserved for Phase 5).

### 8. Assumptions
- **ASSUMPTION**: JavaScript execution, if required, is handled by a separate Rendering Phase. The Baseline Generation Phase only consumes finalized content snapshots.
- **ASSUMPTION**: The system-wide `extraction_version` is managed via external configuration.
