# Phase 4: Baseline Management

## Purpose
To establish the "Known Good" state of a page using **Normalized Content**.

## Definition of Baseline
A Baseline is no longer a raw HTML file.
**Baseline = Normalized Content + Hash + Version Metadata.**

## Responsibilities
1.  **Promotion**: Accepting a `PageVersion` as the new Baseline.
2.  **Versioning**: Tracking when a baseline changed (Historical Hooks: `promoted_at`).
3.  **Storage**: Persisting the baseline record (Archiving old baselines, NEVER overwriting).

## Workflow
1.  **Initial Crawl**:
    *   System sees URL for the first time.
    *   Phase 1-3 run.
    *   Phase 4 accepts the result as `Baseline v1`.
2.  **Re-Baseline**:
    *   User manually marks a "Defacement" as "Actually a Update".
    *   System promotes the current `PageVersion` to `Baseline v2`.

## Critical Change
*   **OLD**: Baseline was a saved HTML file on disk.
*   **NEW**: Baseline is a record referencing a specific `PageVersion` (via `page_version_id`), with `promoted_at` timestamp.

## Input
*   **Normalized Content** + **Hash**.

## Output
*   **DB Record**: A stored baseline entry.
