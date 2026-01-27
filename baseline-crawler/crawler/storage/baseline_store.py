# crawler/storage/baseline_store.py

from pathlib import Path
from crawler.storage.db import insert_defacement_site
from crawler.storage.mysql import upsert_baseline_hash, fetch_baseline_hash
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


def save_baseline(*, custid, siteid, url, html, base_url=None):
    """
    Creates or UPDATES the baseline for a URL.

    Returns:
        (baseline_id, path, action)
        action ∈ {"created", "updated"}
    """

    normalized_url = normalize_url(url, preference_url=base_url)

    from compare_utils import semantic_hash
    content_hash = semantic_hash(html)

    # 1️⃣ Check if baseline already exists (for logging only)
    existing = fetch_baseline_hash(
        site_id=siteid,
        normalized_url=normalized_url,
        base_url=base_url,
    )

    action = "updated" if existing else "created"

    # 2️⃣ Prepare directory
    site_dir = BASELINE_ROOT / str(custid) / str(siteid)
    site_dir.mkdir(parents=True, exist_ok=True)

    # 3️⃣ Stable baseline ID per URL
    url_fingerprint = sha256(normalized_url)[:12]
    baseline_id = f"{siteid}-{url_fingerprint}"
    path = site_dir / f"{baseline_id}.html"

    print(
    f"[BASELINE] {action.upper()} baseline "
    f"id={baseline_id} url={url}"
)


    # 4️⃣ UPSERT baseline (always)
    upsert_baseline_hash(
        site_id=siteid,
        normalized_url=normalized_url,
        content_hash=content_hash,
        baseline_path=str(path),
        base_url=base_url,
    )

    # 5️⃣ Overwrite file
    path.write_text(html.strip(), encoding="utf-8")

    # 6️⃣ Link defacement site (idempotent)
    insert_defacement_site(
        siteid=siteid,
        baseline_id=baseline_id,
        url=url,
        base_url=base_url,
    )

    return baseline_id, str(path), action

