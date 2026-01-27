# crawler/storage/baseline_store.py

from pathlib import Path
from crawler.storage.db import insert_defacement_site
from crawler.storage.mysql import upsert_baseline_hash, fetch_baseline_hash
from crawler.normalizer import normalize_url
from crawler.content_fingerprint import semantic_hash

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


def save_baseline(*, custid, siteid, url, html, base_url=None):
    """
    Creates or UPDATES baseline for a URL.
    - Reuses existing baseline_id if present
    - Creates a new baseline_id only once per URL
    """

    normalized_url = normalize_url(url, preference_url=base_url)

    content_hash = semantic_hash(html)

    # --------------------------------------------------
    # 1️⃣ Check if baseline already exists for this URL
    # --------------------------------------------------
    existing = fetch_baseline_hash(
        site_id=siteid,
        normalized_url=normalized_url,
        base_url=base_url,
    )

    site_dir = BASELINE_ROOT / str(custid) / str(siteid)
    site_dir.mkdir(parents=True, exist_ok=True)

    if existing and existing.get("baseline_path"):
        # 🔁 UPDATE EXISTING BASELINE: reuse same file name and path
        path = Path(existing["baseline_path"])
        baseline_id = path.stem
        # Ensure parent exists before overwrite
        path.parent.mkdir(parents=True, exist_ok=True)
        action = "updated"
    else:
        # 🆕 CREATE NEW BASELINE ID ONCE
        baseline_id = _next_baseline_id(site_dir, siteid)
        path = site_dir / f"{baseline_id}.html"
        action = "created"

    print(
        f"[BASELINE] {action.upper()} baseline "
        f"id={baseline_id} url={url}"
    )

    # --------------------------------------------------
    # 2️⃣ UPSERT baseline record
    # --------------------------------------------------
    upsert_baseline_hash(
        site_id=siteid,
        normalized_url=normalized_url,
        content_hash=content_hash,
        baseline_path=str(path),
        base_url=base_url,
    )

    # --------------------------------------------------
    # 3️⃣ Overwrite the SAME file every time
    # --------------------------------------------------
    path.write_text(html.strip(), encoding="utf-8")

    # --------------------------------------------------
    # 4️⃣ Ensure defacement_sites points to SAME baseline_id
    # --------------------------------------------------
    insert_defacement_site(
        siteid=siteid,
        baseline_id=baseline_id,
        url=url,
        base_url=base_url,
    )

    return baseline_id, str(path), action


