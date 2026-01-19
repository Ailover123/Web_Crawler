# Phase 5: Detection

## Purpose
To identify unauthorized changes (defacements) by comparing the current fetch against the baseline.

## Logic Flow
1.  **Fetch Baseline**: Retrieve `BaselineHash` for the URL.
2.  **Compare**:
    *   `CurrentHash == BaselineHash`: **MATCH**. No Change.
        *   Action: Update `last_seen` timestamp. **Discard content content logic** (Do not store duplicate body).
    *   `CurrentHash != BaselineHash`: **MISMATCH**. Potential Defacement.
        *   Action: Compute Diff.
        *   Action: Compute Diff.
        *   Action: Store `NewPageVersion` (Normalized Content).
        *   Action: Create `DetectionEvent` (Linked to `previous_baseline_version_id` & `current_page_version_id`).

## Defacement Definition
**Defacement is defined as Normalized Hash Drift.**
*   If the raw HTML changes but the normalized content is identical (e.g., ad ID changed), the hashes will match. **This is SAFE.**
*   If visual text or structure changes, the hashes will differ. **This is ALERT.**

## Input
*   **Current Hash** (Phase 3).
*   **Current Normalized Content** (Phase 2).
*   **Baseline Hash** (DB).

## Output
*   **Verdict**: SAFE / SUSPICIOUS.
*   **Persistence**: Only persists `PageVersion` if a mismatch occurred.

## Why this is efficient
The vast majority of crawls result in a MATCH. In these cases, we store NOTHING except a timestamp update.
