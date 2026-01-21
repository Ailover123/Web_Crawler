# crawler/storage/baseline_store.py

from pathlib import Path
from crawler.storage.db import insert_defacement_site
from crawler.storage.mysql import upsert_baseline_hash
from crawler.normalizer import normalize_html, normalize_url
from crawler.hasher import sha256

import threading

BASELINE_ROOT = Path("baselines")

# Global lock and cache for sequence numbers
_ID_LOCK = threading.Lock()
_SITE_MAX_IDS = {}


def _next_baseline_id(site_dir: Path, siteid: int) -> str:
    """
    Thread-safe generation of the next baseline ID.
    Uses an in-memory cache to avoid O(N) disk scans on every write.
    """
    with _ID_LOCK:
        if siteid not in _SITE_MAX_IDS:
            max_seq = 0
            prefix = f"{siteid}-"
            
            if site_dir.exists():
                for f in site_dir.glob(f"{siteid}-*.html"):
                    try:
                        stem = f.stem
                        if stem.startswith(prefix):
                            num = int(stem[len(prefix):])
                            if num > max_seq:
                                max_seq = num
                    except ValueError:
                        pass
            
            _SITE_MAX_IDS[siteid] = max_seq

        _SITE_MAX_IDS[siteid] += 1
        return f"{siteid}-{_SITE_MAX_IDS[siteid]}"


def save_baseline_if_unique(*, custid, siteid, url, html, base_url=None):
    """
    Tries to insert the baseline hash into the DB FIRST.
    If the DB accepts it (new unique content for this URL), writes the file to disk.
    If the DB rejects it (duplicate URL), skips writing the file.
    
    Returns:
        (baseline_id, path_str) if saved.
        (None, None) if duplicate/skipped.
    """
    # 1. Prepare Data
    site_dir = BASELINE_ROOT / str(custid) / str(siteid)
    site_dir.mkdir(parents=True, exist_ok=True)

    # Note: We generate a candidate ID. If we don't use it (duplicate), 
    # we just burn the sequence number. This is fine.
    baseline_id = _next_baseline_id(site_dir, siteid)
    path = site_dir / f"{baseline_id}.html"
    
    normalized_url = normalize_url(url, preference_url=base_url)
    content_hash = sha256(normalize_html(html))

    # 2. Try DB Insert
    inserted = upsert_baseline_hash(
        site_id=siteid,
        normalized_url=normalized_url,
        content_hash=content_hash,
        baseline_path=str(path),
        base_url=base_url,
    )

    if not inserted:
        return None, None

    # 3. Write File (Only if DB accepted)
    path.write_text(html.strip(), encoding="utf-8")

    # 4. Link to Defacement Site (Legacy logic, but correct for Reference)
    # This table tracks "Seed URLs that have baselines". 
    # If the URL is redundant, we don't add it here either.
    insert_defacement_site(
        siteid=siteid,
        baseline_id=baseline_id,
        url=url,
        base_url=base_url,
    )

    return baseline_id, str(path)
