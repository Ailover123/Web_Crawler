# crawler/storage/baseline_store.py

from pathlib import Path
from crawler.storage.db import insert_defacement_site
from crawler.storage.mysql import upsert_baseline_hash
from crawler.normalizer import normalize_html
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
        # Lazy initialization: scan disk only once per site per run
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

        # Increment and return unique ID
        _SITE_MAX_IDS[siteid] += 1
        return f"{siteid}-{_SITE_MAX_IDS[siteid]}"


def store_snapshot_file(*, custid, siteid, url, html, crawl_mode, base_url=None):
    site_dir = BASELINE_ROOT / str(custid) / str(siteid)
    site_dir.mkdir(parents=True, exist_ok=True)

    baseline_id = _next_baseline_id(site_dir, siteid)
    path = site_dir / f"{baseline_id}.html"
    path.write_text(html.strip(), encoding="utf-8")

    if crawl_mode.upper() == "BASELINE":
        insert_defacement_site(
            siteid=siteid,
            baseline_id=baseline_id,
            url=url,
            base_url=base_url,
        )

    return baseline_id, path.name, str(path)


def store_baseline_hash(*, site_id, normalized_url, raw_html, baseline_path, base_url=None):
    content_hash = sha256(normalize_html(raw_html))

    upsert_baseline_hash(
        site_id=site_id,
        normalized_url=normalized_url,
        content_hash=content_hash,
        baseline_path=baseline_path,
        base_url=base_url,
    )

    return content_hash
