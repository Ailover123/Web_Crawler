from pathlib import Path
from urllib.parse import urlparse
import json
import threading

BASELINE_ROOT = Path("baselines")
OBSERVED_ROOT = Path("observed")

_lock = threading.Lock()


def _get_root_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def _get_or_create_site_folder(root: Path, custid: int, url: str) -> str:
    cust_dir = root / str(custid)
    cust_dir.mkdir(parents=True, exist_ok=True)

    index_file = cust_dir / "index.json"
    root_domain = _get_root_domain(url)

    with _lock:
        data = json.loads(index_file.read_text()) if index_file.exists() else {}

        if root_domain in data:
            return data[root_domain]

        site_number = len(data) + 1
        site_folder_id = f"{custid}{site_number:02d}"
        data[root_domain] = site_folder_id

        index_file.write_text(
            json.dumps(data, indent=2),
            encoding="utf-8"
        )

    return site_folder_id


def _get_next_page_filename(site_dir: Path, site_folder_id: str) -> str:
    index_file = site_dir / "index.json"

    counter = (
        json.loads(index_file.read_text()).get("counter", -1) + 1
        if index_file.exists()
        else 0
    )

    index_file.write_text(
        json.dumps({"counter": counter}, indent=2),
        encoding="utf-8"
    )

    return (
        f"{site_folder_id}.html"
        if counter == 0
        else f"{site_folder_id}-{counter}.html"
    )


def _canonicalize_for_storage(html: str) -> str:
    # Convert escaped newlines (defensive)
    if "\\n" in html:
        html = html.replace("\\n", "\n")

    return html.strip()


def store_snapshot_file(*, custid: int, url: str, html: str, crawl_mode: str):
    crawl_mode = crawl_mode.upper()
    root = BASELINE_ROOT if crawl_mode == "BASELINE" else OBSERVED_ROOT

    site_folder_id = _get_or_create_site_folder(root, custid, url)
    site_dir = root / str(custid) / site_folder_id
    site_dir.mkdir(parents=True, exist_ok=True)

    page_name = _get_next_page_filename(site_dir, site_folder_id)
    path = site_dir / page_name

    html = _canonicalize_for_storage(html)

    # ✅ WRITE RAW HTML (THIS IS THE FIX)
    path.write_text(
        html,
        encoding="utf-8",
        newline="\n"
    )

    return site_folder_id, page_name, str(path)


def load_all_baseline_pages(custid: int, site_folder_id: str) -> list[str]:
    """
    Returns raw HTML strings.
    """
    site_dir = BASELINE_ROOT / str(custid) / site_folder_id
    if not site_dir.exists():
        return []

    pages = []
    for f in site_dir.glob("*.html"):
        try:
            pages.append(f.read_text(encoding="utf-8"))
        except Exception:
            continue

    return pages
