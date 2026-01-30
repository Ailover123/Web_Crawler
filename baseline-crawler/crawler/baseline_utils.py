from pathlib import Path
from crawler.normalizer import normalize_url

BASELINE_ROOT = Path("baselines")


def safe_baseline_filename(url: str) -> str:
    return normalize_url(url)\
        .replace("://", "__")\
        .replace("/", "_")\
        .replace("?", "_")\
        .replace("&", "_")
