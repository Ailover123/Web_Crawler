# Responsibilities:
# - initialize database
# - initialize crawl queue
# - orchestrate crawling
# - create baselines when missing
# - detect defacement when baseline exists
# - store forensic diff evidence
from crawler.storage.url_store import get_all_urls
from crawler.config import SEED_URLS, DEPTH_LIMIT, DATA_DIR
from crawler.queue import CrawlQueue
from crawler.fetcher import fetch
from crawler.parser import parse_html
from crawler.hasher import hash_content, hash_json_keys
from crawler.detection.detector import detect_defacement
from crawler.storage.db import initialize_db,get_connection
from crawler.storage.url_store import insert_url, update_crawl_metadata, url_exists
from crawler.storage.baseline_store import store_baseline
from crawler.storage.diff_store import store_diff
from urllib.parse import urlparse
from crawler.normalizer import (
    strip_trivial_comments,
    normalize_html,
    semantic_normalize_html,
    normalize_url,
)
from pathlib import Path
import os
from datetime import datetime

# Use shared `normalize_url` from crawler.normalizer

pages_crawled = 0

def canonicalize_html(raw_html):
    """Apply a stable canonicalization pipeline used for both baseline and observed HTML.

    Steps:
    - strip trivial cache/footer comments
    - parse with BeautifulSoup
    - normalize DOM (normalize_html)
    - semantic normalize (semantic_normalize_html)
    """
    if not raw_html:
        return ''
    try:
        cleaned = strip_trivial_comments(raw_html)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(cleaned, 'html.parser')
        normalized_dom = normalize_html(soup)
        canonical_html = semantic_normalize_html(str(normalized_dom))
        return canonical_html
    except Exception:
        try:
            return semantic_normalize_html(raw_html)
        except Exception:
            return raw_html

def main():
    global pages_crawled
    # 1. Initialize database schema (safe to call multiple times)
    initialize_db()

    # Decide run mode once: baseline creation (first run) vs monitoring
    conn = get_connection()
    cursor = conn.cursor()
    baseline_count = cursor.execute("SELECT COUNT(*) FROM baseline").fetchone()[0]
    cursor.close()
    conn.close()

    IS_BASELINE_MODE = (baseline_count == 0)
    if IS_BASELINE_MODE:
        print("[MODE] BASELINE CREATION")
    else:
        print("[MODE] MONITORING")

    # 2. Initialize crawl queue with depth limit
    queue = CrawlQueue(max_depth=DEPTH_LIMIT)

    # 3. Seed the queue and URL inventory
    existing_urls = get_all_urls()
    if IS_BASELINE_MODE:
        # Baseline creation run: seed only the configured SEED_URLS
        for seed_url in SEED_URLS:
            # Normalize seed URLs before inserting and enqueueing to ensure consistent visited keys
            nseed = normalize_url(seed_url)
            try:
                insert_url(nseed, crawl_depth=0)
            except Exception:
                pass
            queue.enqueue(nseed, depth=0)
    else:
        # Monitoring run → re-crawl all known URLs (active & crawled)
        for url, depth in existing_urls:
            queue.enqueue(url, depth)

    # 4. Crawl loop (Breadth-First)
    while not queue.is_empty():
        item = queue.dequeue()
        if item is None:
            break

        url, depth = item
        print(f"[CRAWL] {url} (depth={depth})")

        # 5. Fetch HTTP response
        response = fetch(url)
        # New failure handling: fetch returns a dict with ok=False on failure
        if isinstance(response, dict) and response.get('ok') is False:
            update_crawl_metadata(url, status="fetch_failed")
            # TODO: store response['error'] and response['status_code'] for diagnostics
            print(f"[FETCH FAILED] {url} → {response}")
            continue
        if response is None:
            # Unexpected None — treat as fetch failure
            update_crawl_metadata(url, status="fetch_failed")
            print(f"[FETCH FAILED] {url} → None response")
            continue

        pages_crawled += 1
        # Stop if we hit the max pages threshold
        from crawler.config import MAX_PAGES
        if pages_crawled >= MAX_PAGES:
           print(f"[STOP] Reached maximum page limit: {MAX_PAGES}")
           break


        # 6. Detect content type (HTML vs JSON)
        content_type = response.headers.get('Content-Type', '').lower()
        is_json = False
        observed_data = {}

        if 'application/json' in content_type or response.text.strip().startswith(('{', '[')):
            # Lightweight JSON handling: hash JSON key structure only
            try:
                parsed_json = response.json()
                json_hash = hash_json_keys(parsed_json)
                observed_data['html_hash'] = json_hash
                observed_data['script_sources'] = []
                observed_data['script_count'] = 0
                # no links to follow from JSON
                discovered_links = []
                is_json = True
            except Exception:
                # Fallback to HTML parsing if JSON parse fails
                cleaned = strip_trivial_comments(response.text)
                soup, discovered_links, script_sources, script_count = parse_html(cleaned, base_url=url)
                # Use canonical pipeline for hashing and snapshots
                canonical_html = canonicalize_html(response.text)
                html_hash = hash_content(canonical_html)
                observed_data = {
                    'html_hash': html_hash,
                    'normalized_html': canonical_html,
                    'script_sources': script_sources,
                    'script_count': script_count
                }
        else:
            # 7. Parse HTML and extract structure
            # Strip trivial cache/footer comments before parsing so these don't
            # affect normalization/hashing.
            cleaned = strip_trivial_comments(response.text)
            soup, discovered_links, script_sources, script_count = parse_html(cleaned, base_url=url)

            # 8. Parse and canonicalize HTML for hashing (must be identical to baseline canonicalization)
            # Keep parse_html to extract links and script sources, but compute canonical HTML from raw response
            canonical_html = canonicalize_html(response.text)
            html_hash = hash_content(canonical_html)

            observed_data = {
                "html_hash": html_hash,
                "normalized_html": canonical_html,
                "script_sources": script_sources,
                "script_count": script_count,
            }

        # 9. Mode-enforced behavior: do NOT perform detection during baseline runs
        if IS_BASELINE_MODE:
            # BASELINE RUN: create/update baseline records for every crawled URL
            try:
                # Ensure baseline is created from the canonical HTML used for hashing
                # If observed_data contains canonical normalized HTML, use it; otherwise skip snapshot
                canonical_html = observed_data.get('normalized_html')
                html_hash = observed_data.get('html_hash')

                store_baseline(
                    url=url,
                    html_hash=html_hash,
                    script_sources=observed_data.get('script_sources', []),
                    script_count=observed_data.get('script_count')
                )
                print(f"[BASELINE CREATED] {url}")
                # Save canonical baseline snapshot (must match the canonical HTML hashed above)
                try:
                    if canonical_html and html_hash:
                        baseline_dir = Path(DATA_DIR) / "snapshots" / "baselines"
                        baseline_dir.mkdir(parents=True, exist_ok=True)
                        baseline_path = baseline_dir / f"baseline_{html_hash}.html"
                        baseline_path.write_text(canonical_html, encoding="utf-8")
                except Exception:
                    pass
            except Exception:
                pass
        else:
            # MONITORING RUN: perform detection only
            detection_result = detect_defacement(url, observed_data)

            if detection_result and detection_result.get('defaced'):
                diff_summary = {
                    "html_changed": detection_result.get("html_changed"),
                    "scripts_added": detection_result.get("scripts_added", []),
                    "scripts_removed": detection_result.get("scripts_removed", []),
                    "severity_reason": detection_result.get('severity_reason'),
                    "severity_rule": detection_result.get('severity_rule'),
                }

                # Save observed snapshot and include short excerpts in diff_summary for UI review
                try:
                    snapshots_obs_dir = os.path.join(DATA_DIR, 'snapshots', 'observed')
                    os.makedirs(snapshots_obs_dir, exist_ok=True)
                    ob_hash = observed_data.get('html_hash') or 'unknown'
                    from datetime import timezone, timedelta
                    ist = timezone(timedelta(hours=5, minutes=30))
                    ts = datetime.now(ist).strftime('%Y%m%dT%H%M%S%z')
                    obs_path = os.path.join(snapshots_obs_dir, f"observed_{ts}_{ob_hash}.html")
                    # Write observed semantically-normalized HTML when available
                    # Prefer semantically-normalized HTML from observed_data when available
                    content_to_write = observed_data.get('normalized_html') or (response.text if hasattr(response, 'text') else '')
                    with open(obs_path, 'w', encoding='utf-8') as fh:
                        fh.write(content_to_write)

                    baseline_excerpt = ''
                    baseline_snapshot_path = os.path.join(DATA_DIR, 'snapshots', 'baselines', f"baseline_{detection_result.get('baseline_hash')}.html")
                    if os.path.exists(baseline_snapshot_path):
                        try:
                            with open(baseline_snapshot_path, 'r', encoding='utf-8') as bf:
                                baseline_excerpt = bf.read()[:4000]
                        except Exception:
                            baseline_excerpt = ''

                    observed_excerpt = content_to_write[:4000]
                    diff_summary['baseline_excerpt'] = baseline_excerpt
                    diff_summary['observed_excerpt'] = observed_excerpt
                    diff_summary['observed_snapshot_path'] = obs_path
                except Exception:
                    pass

                store_diff(
                    url=url,
                    baseline_hash=detection_result["baseline_hash"],
                    observed_hash=detection_result["observed_hash"],
                    diff_summary=diff_summary,
                    severity=detection_result["severity"],
                )
                print(f"[DEFACEMENT DETECTED] {url}")
            else:
                print(f"[OK] No change detected for {url}")

        # 10. Enqueue newly discovered links
        normalized_current = normalize_url(url)
        allowed_domain = urlparse(normalized_current).netloc

        for link in discovered_links:
            # Normalize each discovered link for consistent comparison and deduping
            nlink = normalize_url(link)
            parsed = urlparse(nlink)
            # Only crawl pages from the same domain (do not follow external links)
            if parsed.netloc == allowed_domain:
                # Skip blocked extensions before touching DB
                if not queue.is_allowed_to_crawl(nlink):
                    continue

                # Do not enqueue URLs already visited in this run or already queued
                if queue.is_queued(nlink):
                    continue
                if nlink in queue.visited:
                    continue

                # If URL already exists in DB, skip only during baseline creation
                try:
                    if IS_BASELINE_MODE and url_exists(nlink):
                        continue
                except Exception:
                    # If DB check fails, fall back to queue-level dedupe
                    pass

                # Insert into URL inventory so UI sees discovered URLs and so future
                # runs can dedupe via DB.
                try:
                    insert_url(nlink, crawl_depth=depth+1)
                except Exception:
                    pass

                queue.enqueue(nlink, depth + 1)

        # 11. Update crawl metadata
        update_crawl_metadata(url, status="crawled")
    # End of crawl summary (use DB to compute final counts)
    conn = get_connection()
    cursor = conn.cursor()
    # Compute summary counts using existing tables
    seed_domain = None
    if SEED_URLS:
        try:
            seed_domain = urlparse(SEED_URLS[0]).netloc
        except Exception:
            seed_domain = SEED_URLS[0]

    urls_crawled = cursor.execute("SELECT COUNT(*) FROM urls WHERE status = 'crawled'").fetchone()[0]
    baselines_created = cursor.execute("SELECT COUNT(*) FROM baseline").fetchone()[0]
    defacements = cursor.execute("SELECT COUNT(*) FROM diff_evidence").fetchone()[0]
    high_sev = cursor.execute("SELECT COUNT(*) FROM diff_evidence WHERE severity = 'high'").fetchone()[0]
    medium_sev = cursor.execute("SELECT COUNT(*) FROM diff_evidence WHERE severity = 'medium'").fetchone()[0]
    fetch_failures = cursor.execute("SELECT COUNT(*) FROM urls WHERE status = 'fetch_failed'").fetchone()[0]

    cursor.close()
    conn.close()

    print("[Crawl Complete]")
    print(f"Seed Domain: {seed_domain}")
    print(f"URLs Crawled: {urls_crawled}")
    print(f"Baselines Created: {baselines_created}")
    print(f"Defacements Detected: {defacements}")
    print(f"High Severity: {high_sev}")
    print(f"Medium Severity: {medium_sev}")
    print(f"Fetch Failures: {fetch_failures}")
 
if __name__ == "__main__":
    main()
