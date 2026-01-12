from pathlib import Path

BASELINE_ROOT = Path("baselines")


def load_latest_baseline_html(custid: int, site_folder_id: str) -> str | None:
    """
    Loads the most recent baseline HTML for a site.
    Returns None if baseline does not exist.
    """
    site_dir = BASELINE_ROOT / str(custid) / site_folder_id

    if not site_dir.exists():
        return None

    html_files = sorted(site_dir.glob("*.html"))
    if not html_files:
        return None

    # Last file = latest baseline snapshot
    return html_files[-1].read_text(encoding="utf-8", errors="ignore")
