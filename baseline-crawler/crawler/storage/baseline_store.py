# crawler/storage/baseline_store.py

from pathlib import Path
from crawler.storage.db import insert_defacement_site
from crawler.storage.mysql import (
    upsert_baseline_hash,
    fetch_baseline_hash,
    site_has_baselines,
)
from crawler.processor import LinkUtility, ContentNormalizer
from crawler.core import logger

import threading

BASELINE_ROOT = Path("baselines")

# Global lock and cache for sequence numbers
_ID_LOCK = threading.Lock()
_SITE_MAX_IDS = {}
_SITE_HAS_BASELINES = {}


def _next_baseline_id(site_dir: Path, siteid: int, *, force_reset: bool = False, reuse_if_orphaned: bool = False) -> str:
    """
    Thread-safe generation of the next baseline ID.
    Uses an in-memory cache to avoid O(N) disk scans on every write.
    """
    with _ID_LOCK:
        if force_reset:
            _SITE_MAX_IDS.pop(siteid, None)

        if siteid not in _SITE_MAX_IDS:
            max_seq = 0
            prefix = f"{siteid}-"
            
            # If we are strictly overwriting (first run or reset), we start from 0
            # AND we tolerate if files exist (we claim the IDs sequentially).
            if not force_reset and not reuse_if_orphaned and site_dir.exists():
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


def save_baseline(*, custid, siteid, url, html, base_url=None):
    """
    Creates or UPDATES baseline for a URL.
    - Reuses existing baseline_id if present
    - Creates a new baseline_id only once per URL
    """

    normalized_url = LinkUtility.normalize_url(url, preference_url=base_url)

    content_hash = ContentNormalizer.semantic_hash(html)

    site_dir = BASELINE_ROOT / str(custid) / str(siteid)
    site_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------
    # 1️⃣ Check if baseline already exists for this URL
    # --------------------------------------------------
    existing = fetch_baseline_hash(
        site_id=siteid,
        normalized_url=normalized_url,
        base_url=base_url,
    )

    if siteid not in _SITE_HAS_BASELINES:
        _SITE_HAS_BASELINES[siteid] = site_has_baselines(siteid)

    if existing and existing.get("baseline_path"):
        # 🔁 UPDATE EXISTING BASELINE
        baseline_id = Path(existing["baseline_path"]).stem
        action = "updated"
    else:
        # 🆕 CREATE NEW BASELINE ID ONCE
        # If this is the FIRST baseline for this site in this run/context (reset_sequence=True),
        # we allow overwriting files 1, 2, 3... treating them as 'orphaned' if DB doesn't know them.
        reset_sequence = not _SITE_HAS_BASELINES.get(siteid, False)
        
        baseline_id = _next_baseline_id(
            site_dir,
            siteid,
            force_reset=reset_sequence,
            reuse_if_orphaned=reset_sequence, 
        )
        action = "created"
        _SITE_HAS_BASELINES[siteid] = True

    path = site_dir / f"{baseline_id}.html"

    logger.info(
        f"[BASELINE] {action.upper()} baseline "
        f"id={baseline_id} for url={url}"
    )

    # --------------------------------------------------
    # 2️⃣ UPSERT baseline record
    # --------------------------------------------------
    upsert_baseline_hash(
        site_id=siteid,
        normalized_url=normalized_url,
        content_hash=content_hash,
        baseline_path=str(path),
        baseline_id=baseline_id,
        base_url=base_url,
    )

    # --------------------------------------------------
    # 3️⃣ Overwrite the SAME file every time
    # --------------------------------------------------
    path.write_text(html.strip(), encoding="utf-8")

    return baseline_id, str(path), action
