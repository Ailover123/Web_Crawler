# ARCHITECTURE LOCKED — Rendering / JavaScript Enrichment Phase
# Changes require architecture review.

# Phase 6: Rendering / JavaScript Enrichment Phase Interface

### 1. Input Schema
This phase consumes raw artifacts and applies optional execution policies.

#### 1.1 `crawl_artifact` (Required)
| Field | Type | Description |
| :--- | :--- | :--- |
| `normalized_url` | String | Stability identity. |
| `raw_body` | Bytes | Raw HTML source to be injected into the rendering engine. |
| `http_status` | Integer | Eligibility Gate: Should be 200-299 for meaningful rendering. |
| `content_type` | String | Must be `text/html` or similar interactive type. |

#### 1.2 `rendering_policy` (Optional)
| Field | Type | Requirement | Description |
| :--- | :--- | :--- | :--- |
| `wait_until` | Enum | Optional | [LOAD, DOMCONTENTLOADED, NETWORK_IDLE]. |
| `execution_timeout_ms`| Integer | Optional | Max time for JS execution. |
| `viewport_width` | Integer | Optional | Screen width for responsive layout rendering. |
| `viewport_height` | Integer | Optional | Screen height for responsive layout rendering. |

#### 1.3 Eligibility Rules
- **Supported Types**: Rendering MUST only run if `content_type` is identified as a renderable type (e.g. `text/html`, `application/xhtml+xml`).
- **Status Filter**: Rendering SHOULD be bypassed for non-2xx status codes unless explicitly forced.

---

### 2. Output Schema: `RenderedArtifact`
An immutable snapshot of the browser's final memory state.

| Field | Type | Requirement | Description |
| :--- | :--- | :--- | :--- |
| `normalized_url` | String | Required | Identity matching the parent `crawl_artifact`. |
| `rendered_artifact_id`| UUID/String | Required | Unique ID for this specific execution result. |
| `parent_artifact_id` | String | Required | Explicit link to the source `CrawlArtifact`. |
| `rendered_body` | Bytes/String | Required | The final serialized DOM state (HTML). |
| `screenshot` | Blob/Bytes | Optional | Visual snapshot of the rendered state. |
| `js_error_log` | List<Text> | Required | Capture of console errors or execution warnings. |
| `render_duration_ms` | Integer | Required | Total wall-clock time for script execution. |
| `render_timestamp` | ISO8601 | Required | UTC completion time. |

---

### 3. Responsibilities
- **Isolated Execution**: Executing the `raw_body` in a sandbox with restricted network access.
- **Serialization**: Capturing the current DOM state after script execution/settling.
- **Resource Management**: Handling browser timeouts and process lifecycle.

---

### 4. Rendering Semantics
- **Determinism**: The phase aims for "High Probability Determinism". External noise (stochastic timing) must be minimized via `wait_until` triggers.
- **Resource Constraints**: Browsers must be capped by `execution_timeout_ms`.
- **Lazy Loading**: The phase must attempt to trigger/scroll for lazy-loaded elements if mandated by `rendering_policy`.

---

### 5. Failure Semantics
Errors in rendering should not result in a system crash but must be clearly labeled for the Analysis Phase.

| Failure Mode | Status Code | External Behavior |
| :--- | :--- | :--- |
| **JS Timeout** | `RENDER_TIMEOUT` | Return `RenderedArtifact` with partial DOM and warning. |
| **Crash/OOM** | `RENDER_FAILED` | Emit failure status; no artifact produced. |
| **Unsupported Type**| `INELIGIBLE_TYPE` | Emit skip status; no artifact produced. |

---

### 6. Invariants
- **Source Integrity**: The phase MUST NOT modify the original `CrawlArtifact`.
- **Statelessness**: No cookies, local storage, or session state may persist between rendering tasks.
- **Linkage**: Every `RenderedArtifact` must be uniquely traceable to exactly one `CrawlArtifact`.

---

### 7. Forbidden Behaviors
- **No Secondary Crawling**: The browser must not navigate to new URLs or initiate cross-origin fetches (unless explicitly allowed for resource loading).
- **No Baseline Logic**: Must not perform feature extraction or hashing (Reserved for Phase 4).
- **No Self-Correction**: Must not attempt to "fix" or sanitize the DOML; just serialize it.

---

### 8. Assumptions
- **ASSUMPTION**: Resource loading (CSS/Images) may require limited network access from the rendering sandbox; this is separate from "Crawling".
- **ASSUMPTION**: The downstream `Baseline Generation Phase` will be updated to accept `RenderedArtifact` as an alternative to `CrawlArtifact`. Winslow
