# ARCHITECTURE LOCKED — Frontier Phase
# Changes require architecture review.

# Phase 2: Frontier & Orchestration Phase Interface

### 1. Task Schema (Unit of Work Identity)
| Field | Type | Requirement | Description |
| :--- | :--- | :--- | :--- |
| `normalized_url` | String | **Primary Key** | The unique unit of work identity. |
| `state` | Enum | Required | [DISCOVERED, PENDING, ASSIGNED, COMPLETED, FAILED] |
| `attempt_count` | Integer | Required | Number of times transitioned to `ASSIGNED`. |
| `last_heartbeat` | Timestamp | Required | Last lease activity (Frontier-managed). |
| `priority` | Integer | Required | Scheduling weight (0 = default). |
| `depth` | Integer | Required | Crawl distance (root = 0). |

### 2. Task States & Allowed Transitions
- **DISCOVERED → PENDING**: Scoping/Domain validation successful.
- **PENDING → ASSIGNED**: Task handed to worker; `attempt_count` increments, `last_heartbeat` initialized.
- **ASSIGNED → COMPLETED**: `CrawlArtifact` received with `http_status > 0`.
- **ASSIGNED → PENDING**: 
    - Lease timeout (no heartbeat within `CRASH_THRESHOLD`).
    - Worker-reported failure (`status=0`) AND `attempt_count < MAX_RETRIES`.
- **ASSIGNED → FAILED**: 
    - Worker-reported failure (`status=0`) AND `attempt_count >= MAX_RETRIES`.

### 3. Deduplication Rules
- **Identity Consistency**: The `normalized_url` is the global index. 
- **Pre-Flight Check**: Every newly discovered URL must be checked against the Task Store. If the `normalized_url` exists in any state, the discovery is discarded immediately.

### 4. Heartbeat & Recovery
- **Frontier Ownership**: The Frontier MUST update `last_heartbeat` when a task enters `ASSIGNED`.
- **Worker Signal**: Crawl Workers may optionally update `last_heartbeat` during long downloads.
- **Lease Reset**: Tasks in `ASSIGNED` with `last_heartbeat` older than `CRASH_THRESHOLD` are reset to `PENDING` automatically.

### 5. Invariants
- **Stable Identity**: `normalized_url` is the only persistent identifier across all stores.
- **Depth Immutability**: `depth` is assigned at `DISCOVERED` and MUST never change after entering `PENDING`.
- **Exclusivity**: A URL must never be held in `ASSIGNED` by more than one worker process.

### 6. Forbidden Behaviors
- **No Direct Fetching**: Frontier must not execute network requests.
- **No Content Analysis**: Frontier must not parse `raw_body` or handle defacement logic.
- **No State Leaks**: Frontier must not modify `CrawlArtifacts` or `BaselineProfiles`.

### 7. Assumptions
- **ASSUMPTION**: `MAX_RETRIES` and `CRASH_THRESHOLD` are global configuration constants.
- **ASSUMPTION**: The Frontier handles "Seed" URL injection via the same `DISCOVERED -> PENDING` flow.
