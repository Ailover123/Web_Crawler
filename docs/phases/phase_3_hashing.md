# Phase 3: Hashing

## Purpose
To generate a compact fingerprint of the **Normalized Content** for efficient storage and comparison.

## Responsibilities
1.  **Compute Hash**: Apply a deterministic hashing algorithm to the output of Phase 2.
2.  **Consistency**: Ensure the same normalized content ALWAYS yields the same hash.

## Algorithm Choice
*   **Primary**: SHA-256 (for exact match modification detection).
*   **Secondary (Optional)**: SimHash / MinHash (for fuzzy similarity, if needed later).
*   *Current Architecture defaults to Exact Hashing (SHA-256).*

## Input
*   **Normalized Content** (String/Bytes).

## Output
*   **ContentHash**: Hex string.

## Logic Flow
```python
def process(normalized_content):
    if not normalized_content:
        return None
    return sha256(normalized_content.encode('utf-8')).hexdigest()
```

## Impact
*   Drastically reduces storage lookups.
*   We compare `Hash(New) vs Hash(Baseline)` instead of `Text vs Text`.
