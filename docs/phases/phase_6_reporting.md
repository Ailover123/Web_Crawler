# Phase 6: Reporting / UI

## Purpose
To visualize changes for the security analyst.

## Constraints
*   **NO RAW REPLAY**: The UI cannot render the page exactly as it looked in the browser (missing CSS, images, JS context).
*   **NORMALIZED DIFF ONLY**: The UI shows a diff of the *Normalized Text/Structure*.

## Supported Views
1.  **Side-by-Side Diff**:
    *   Left: Baseline Normalized Text.
    *   Right: Detected Normalized Text.
    *   Highlights: Added/Removed lines.
2.  **Operational Timeline**:
    *   History of "Safe" checks vs "Alert" events.

## User Expectation Management
*   The UI must clearly state: *"Displaying Normalized Content. Layout and images are stripped for analysis."*
*   **LIMITATION WARNING**: This system is designed for **operational integrity monitoring**, not **legal-grade forensic reconstruction**.
*   Analysts seeking forensic evidence (IPs, raw payloads) must rely on different logs (e.g., WAF logs), as the crawler does not persist raw response bodies.

## Input
*   **Baseline PageVersion**.
*   **Detected PageVersion**.

## Output
*   **Visual Diff** (HTML/Text).
