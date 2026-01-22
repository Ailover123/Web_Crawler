"""
URL and HTML normalization utilities.

⚠️ IMPORTANT:
- URL normalization is conservative (crawler correctness depends on it)
- HTML normalization is ONLY for hashing / comparison
"""

from urllib.parse import urlparse, urlunparse, urljoin
from bs4 import BeautifulSoup


# -------------------------
# URL NORMALIZATION
# -------------------------

def normalize_url(url: str, *, base: str | None = None) -> str:
    if not url:
        return ""

    url = url.strip()

    if base:
        url = urljoin(base, url)

    parsed = urlparse(url)

    scheme = parsed.scheme.lower() if parsed.scheme else "http"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    query = parsed.query

    return urlunparse((
        scheme,
        netloc,
        path,
        "",
        query,
        ""
    ))


# -------------------------
# HTML NORMALIZATION (FOR HASHING / DIFF)
# -------------------------

def normalize_url(url: str, *, base: str | None = None) -> str:
    if not url:
        return ""

    url = url.strip()

    if base:
        url = urljoin(base, url)

    parsed = urlparse(url)

    scheme = parsed.scheme.lower() if parsed.scheme else "http"
    netloc = parsed.netloc.lower()

    # --- FIX: canonicalize path ---
    path = parsed.path or "/"

    # 🔥 REMOVE trailing slash except root
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    query = parsed.query

    return urlunparse((
        scheme,
        netloc,
        path,
        "",
        query,
        ""
    ))

from bs4 import BeautifulSoup

def normalize_html(html: str) -> str:
    """
    Normalize HTML ONLY for hashing / comparison.
    This must be deterministic.
    """
    if not html:
        return ""

    soup = BeautifulSoup(html, "lxml")

    # Remove noisy tags
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # Canonicalize whitespace
    normalized = soup.prettify()
    normalized = "\n".join(
        line.strip() for line in normalized.splitlines() if line.strip()
    )

    return normalized

# -------------------------
# JS RENDER NORMALIZATION
# -------------------------

def normalize_rendered_html(html: str) -> str:
    """
    Cleanup for JS-rendered HTML.
    NOT used for hashing.
    """
    if not html:
        return ""
    if "\\n" in html:
        html = html.replace("\\n", "\n")
    return html.strip()
