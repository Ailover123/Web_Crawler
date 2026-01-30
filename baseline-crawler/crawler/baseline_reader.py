import json
from pathlib import Path

from crawler.normalizer import normalize_url
from crawler.baseline_utils import BASELINE_ROOT, safe_baseline_filename



def load_baseline_file(*, custid: int, siteid: int, url: str):
    """
    Load baseline JSON snapshot for a given customer, site, and URL.
    Returns parsed dict if exists, else None.
    """

    nurl = normalize_url(url)

    base_dir = BASELINE_ROOT / f"cust_{custid}" / f"site_{siteid}"
    baseline_file = base_dir / f"{safe_baseline_filename(url)}.json"


    if not baseline_file.exists():
        return None

    try:
        return json.loads(
            baseline_file.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Corrupt baseline file: {baseline_file}"
        ) from e
