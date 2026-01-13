# ARCHITECTURE LOCKED — Defacement Detection Phase
# Changes require architecture review.

# Phase 5: Defacement Detection Phase Interface

### 1. Input Schema
The phase requires two primary data structures and an optional policy map.

#### 1.1 `crawl_artifact` (Required)
| Field | Type | Description |
| :--- | :--- | :--- |
| `normalized_url` | String | Must match the `baseline_profile.normalized_url`. |
| `raw_body` | Bytes | Raw binary content to be analyzed. |
| `http_status` | Integer | Used to determine if comparison is valid (e.g. status 0/500 vs 200). |
| `content_type` | String | Directs the internal extraction logic. |

#### 1.2 `baseline_profile` (Required)
| Field | Type | Description |
| :--- | :--- | :--- |
| `baseline_id` | String | Unique ID of the ground truth profile. |
| `normalized_url` | String | Must match the `crawl_artifact.normalized_url`. |
| `extraction_version` | String | Version ID of the logic used to create this profile. |
| `structural_digest` | String | The skeleton hash to be compared against. |
| `content_features` | Map<String, Any> | Feature set for quantitative similarity comparison. |

#### 1.3 `detection_policy` (Optional)
| Field | Type | Description |
| :--- | :--- | :--- |
| `thresholds` | Map<String, Float> | Overrides for drift sensitivity (0.0 to 1.0). |
| `ignore_indicators` | List<String> | Heuristics to bypass during classification. |

---

### 2. Output Schema: `DetectionVerdict`
The verdict is an immutable result of a single comparison operation.

| Field | Type | Requirement | Description |
| :--- | :--- | :--- | :--- |
| `verdict_id` | UUID/String | Required | Unique identifier for this specific analysis instance. |
| `normalized_url` | String | Required | Primary identity matching the inputs. |
| `baseline_id` | String | Required | Explicit link to the `baseline_profile` used. |
| `status` | Enum | Required | **Categorical Outcome**: `[CLEAN, POTENTIAL_DEFACEMENT, DEFACED, FAILED]` |
| `severity` | Enum | Required | **Ordinal Risk Level**: `[NONE, LOW, MEDIUM, HIGH, CRITICAL]` |
| `confidence` | Float | Required | Probability metric from `0.0` (None) to `1.0` (Absolute). |
| `structural_drift` | Float | Required | Scalar difference in page skeleton (`0.0` = identical). |
| `content_drift` | Float | Required | Scalar difference in extracted features (`0.0` = identical). |
| `detected_indicators` | List<String> | Required | List of triggered heuristic labels. |
| `analysis_timestamp` | ISO8601 | Required | UTC completion time of the analysis. |

---

### 3. Severity Classification Semantics
Severity is **ordinal and policy-driven**. It represents the risk level assigned after applying the `detection_policy` to the measured drift.

| Severity | Definition | Invariant Constraint |
| :--- | :--- | :--- |
| **NONE** | No drift or drift < `policy.noise_floor`. | `status` MUST be `CLEAN`. |
| **LOW** | Minor volatility (e.g. changing timestamps/ads) or negligible structural drift. | `status` MUST NOT be `CLEAN`. |
| **MEDIUM** | Moderate drift or presence of non-malicious "Uncertain" heuristics. | `status` is typically `POTENTIAL_DEFACEMENT`. |
| **HIGH** | Significant structural collapse or presence of high-confidence signatures. | `status` is typically `DEFACED`. |
| **CRITICAL** | Total content replacement or verified high-impact malicious payload. | `status` MUST be `DEFACED`. |

---

### 4. Failure Semantics
The phase must handle errors without crashing or returning ambiguous nulls.

| Failure Mode | Internal Status | External Behavior |
| :--- | :--- | :--- |
| **Identity Mismatch** | `CONTRACT_ERROR` | Raise Exception (Prevention of cross-talk). |
| **Mismatched Version** | `INCOMPATIBLE_VERSION` | Return `DetectionVerdict` with `status: FAILED`. |
| **Corrupt Artifact** | `PROCESS_FAILED` | Return `DetectionVerdict` with `status: FAILED`. |
| **Missing Baseline** | `NO_BASELINE` | (Handled by caller; Detection requires both inputs). |

---

### 5. Invariants
1. **Determinism**: The same `crawl_artifact` and `baseline_profile` MUST produce the same `DetectionVerdict` under the same `detection_policy`.
2. **Total Immutability**: The phase NEVER modifies inputs. The `DetectionVerdict` is never updated; it is replaced by a new record in a subsequent crawl.
3. **No External Side-Effects**: No network, no file writes (other than logging), and no global state mutation.

---

### 6. Forbidden Behaviors
- **Auto-Baselining**: Never update a `BaselineProfile` internally because "the change looks okay".
- **Dynamic Policy Creation**: Thresholds must be fixed or pass-in; the phase must not "learn" or adjust its own sensitivity per-URL.
- **Cross-URL Comparison**: No logic may reference artifacts or verdicts from other `normalized_url`s.

---

### 7. Assumptions
- **ASSUMPTION**: The Detection Phase performs its own temporary feature extraction on the `crawl_artifact` to ensure the algorithms match the `baseline_profile` perfectly.
- **ASSUMPTION**: `detection_policy` defaults are managed by the Orchestrator or a global configuration store.
