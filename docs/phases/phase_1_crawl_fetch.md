# Phase 1: Crawl & Fetch

## 🚨 CRITICAL RULE: NO STORAGE
This phase fetches data but **MUST NOT STORE IT**. The raw HTML output is transient and exists only in memory to be passed to Phase 2.

## Responsibilities
1.  **Network I/O**: Execute HTTP requests (GET/Headless Render).
2.  **Resilience**: Handle retries, timeouts, and proxy rotation.
3.  **Pass-Through**: Forward the raw response body immediately to Phase 2.

## Input
*   **Canonical URL**: The strictly canonicalized identity (See Architecture #3).
*   **Crawl Config**: User-agent, timeout settings.

## Output
*   **Raw Response Object**:
    *   `url`: The actual URL fetched (after redirects).
    *   `status_code`: HTTP Int.
    *   `content`: Raw bytes (Transient).
    *   `headers`: Response headers (Transient).

## Failure Modes
*   **Network Error**: Retry -> Fail.
*   **Non-200**: Abort processing (do not pass to Phase 2 unless specifically configured for error tracking).

## Differences from Old Architecture
*   **Old**: Fetched -> Stored Raw -> Processed later.
*   **New**: Fetched -> Normalized immediately. Raw content is NEVER persisted.
