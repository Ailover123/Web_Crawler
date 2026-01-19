# Normalized-Only Web Crawler (Engine v2)

> **ARCHITECTURAL HARD DEPRECATION**: Raw HTML is NO LONGER stored. This system operates strictly on Normalized Content.

## Overview

This is a high-performance web crawler designed for **operational integrity monitoring**. It prioritizes speed, cost-efficiency, and noise reduction over forensic replay. 

*   **Goal**: Detect semantic changes (defacements) in O(1) time.
*   **Method**: Canonicalize URL -> Fetch -> Normalize -> Hash -> Compare.
*   **Constraint**: No raw HTML, no screenshots, no DOM replay.

## Core Pipeline

1.  **Phase 1: Fetch**: Downloads raw bytes (Transient RAM only).
2.  **Phase 2: Normalize**: Strips dynamic noise (ads, scripts) to produce a `PageVersion`.
3.  **Phase 3: Hash**: Computes `SHA-256` of the normalized text.
4.  **Phase 4: Baseline**: Checks if this hash is new or known.
5.  **Phase 5: Detection**: Flags mismatching hashes as alerts.

## Key Documentation

*   [Architecture Invariants](ARCHITECTURE.md)
*   [Detailed Pipeline Walkthrough](docs/PIPELINE_WALKTHROUGH.md)
*   [Phase Logic](docs/phases/)

## Quick Stats

*   **Storage Savings**: ~95-99% vs Raw Archival.
*   **Latency**: ~50ms processing time (excluding network).
*   **Identity**: URL Canonicalization is strict and invariant.
