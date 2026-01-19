# Phase 2: Normalization (CRITICAL)

## 🚨 CRITICAL GATE
This is the most important phase in the new architecture. All content MUST be normalized here. There is no bypass.

## Purpose
To strip away "noise" (ads, timestamps, session IDs, tracking pixels) and produce a **Canonical Representation** of the page content.

## Responsibilities
1.  **HTML Parsing**: robust parsing of malformed HTML (e.g., via `lxml` or `BeautifulSoup`).
2.  **Noise Removal**:
    *   Remove `<script>`, `<style>`, `<iframe`, `<noscript>`.
    *   Remove hidden elements (`display: none`).
    *   Remove known dynamic classes/IDs (e.g., `react-id-*`).
3.  **Formatting**:
    *   Collapse whitespace.
    *   Sort attributes (optional, for strict determinism).
    *   Standardize encoding to UTF-8.

## Input
*   **Raw Response Object** (from Phase 1).

## Output
*   **Normalized Content**: A string representing the semantic core of the page.
*   **Metadata**: Title, crucial meta tags.
*   **Normalization Version**: The version string (e.g., "v1.2") of the rules applied. **Critical for comparability.**

## Data Model Implications
*   The output of THIS phase is what eventually gets stored in `PageVersion`.
*   If normalization is too aggressive, we miss defacements.
*   If normalization is too weak, we store noise and get false positives.

## Versioning
*   Normalization rules MUST be versioned (e.g., `v1.0`, `v1.1`).
*   If rules change, old baselines may become invalid.
