# crawler/storage/baseline_store.py

import threading
from pathlib import Path
from crawler.storage.db import insert_defacement_site
from crawler.storage.mysql import upsert_baseline_hash, fetch_baseline_hash, get_connection
from crawler.normalizer import normalize_url
from crawler.logger import logger
from compare_utils import semantic_hash

BASELINE_ROOT = Path("baselines")

# Thread-safe lock for sequence generation
_ID_LOCK = threading.Lock()
_SITE_MAX_IDS = {}


def _get_next_sequence_id(siteid):
    """
    Finds the next available sequence number for a site by scanning the DB once.
    Then uses an in-memory counter for this run.
    """
    with _ID_LOCK:
        if siteid not in _SITE_MAX_IDS:
            conn = get_connection()
            try:
                cur = conn.cursor()
                # Find the highest number after the '-' in the baseline_path/id
                cur.execute(
                    "SELECT baseline_path FROM baseline_pages WHERE site_id = %s", 
                    (siteid,)
                )
                rows = cur.fetchall()
                max_seq = 0
                for row in rows:
                    if row[0]:
                        try:
                            # Path is like 'baselines/101/10106/10106-5.html'
                            stem = Path(row[0]).stem
                            num = int(stem.split("-")[1])
                            if num > max_seq:
                                max_seq = num
                        except (IndexError, ValueError):
                            continue
                _SITE_MAX_IDS[siteid] = max_seq
            finally:
                cur.close()
                conn.close()
                from crawler.storage.db_guard import DB_SEMAPHORE
                DB_SEMAPHORE.release()

        _SITE_MAX_IDS[siteid] += 1
        return f"{siteid}-{_SITE_MAX_IDS[siteid]}"


def save_baseline(*, custid, siteid, url, html, base_url=None):
    """
    Creates or UPDATES baseline for a URL.
    - Reuses existing baseline_id if present
    - Unconditional update (as requested)
    """
    normalized_url = normalize_url(url, preference_url=base_url)
    content_hash = semantic_hash(html)

    # 1. Check for existing record
    existing = fetch_baseline_hash(
        site_id=siteid,
        normalized_url=normalized_url,
        base_url=base_url
    )

    site_dir = BASELINE_ROOT / str(custid) / str(siteid)
    site_dir.mkdir(parents=True, exist_ok=True)

    if existing and existing.get("baseline_path"):
        # REUSE EXISTING ID
        path = Path(existing["baseline_path"])
        baseline_id = path.stem
        action = "updated"
    else:
        # GENERATE NEW SEQUENTIAL ID
        baseline_id = _get_next_sequence_id(siteid)
        path = site_dir / f"{baseline_id}.html"
        action = "created"

    logger.info(f"[BASELINE] {action.upper()} baseline for {url} with ID {baseline_id}")

    # 2. Update Database (Always)
    upsert_baseline_hash(
        site_id=siteid,
        normalized_url=normalized_url,
        content_hash=content_hash,
        baseline_path=str(path),
        base_url=base_url,
    )

    # 3. Write File
    path.write_text(html.strip(), encoding="utf-8")

    # 4. Link to Defacement Site
    insert_defacement_site(
        siteid=siteid,
        baseline_id=baseline_id,
        url=url,
        base_url=base_url,
    )

    return baseline_id, str(path), action


