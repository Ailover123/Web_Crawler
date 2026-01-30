"""
URL and HTML normalization utilities.

IMPORTANT:
- normalize_url() → for FETCHING (network-safe)
- get_canonical_id() → for DB identity (scheme-less, stable)
- normalize_html() → ONLY for hashing / diffing
"""

from urllib.parse import urlparse, urlunparse, urljoin
from bs4 import BeautifulSoup


# ============================================================
# URL NORMALIZATION (FOR FETCHING)
# ============================================================

def normalize_url(
    url: str,
    *,
    base: str | None = None,
    preference_url: str | None = None,
) -> str:
    """
    Normalize a URL for fetching.

    Guarantees:
    - Always returns a FULL URL with scheme
    - Forces HTTPS
    - Normalizes www/non-www to match preference_url if equivalent
    - Removes trailing slash (except root)
    """

    if not url:
        return ""

    url = url.strip()

    # If scheme missing but looks like domain/path
    if "://" not in url and not url.startswith("/"):
        url = "http://" + url

    # Resolve relative URLs
    if base:
        url = urljoin(base, url)

    parsed = urlparse(url)

    # Force HTTPS
    scheme = "https"
    netloc = parsed.netloc.lower()

    # Apply domain preference (www vs non-www)
    if preference_url:
        pref = urlparse(
            preference_url
            if "://" in preference_url
            else "https://" + preference_url
        )

        clean_netloc = netloc.split(":")[0]
        clean_pref = pref.netloc.lower().split(":")[0]

        base_netloc = clean_netloc[4:] if clean_netloc.startswith("www.") else clean_netloc
        base_pref = clean_pref[4:] if clean_pref.startswith("www.") else clean_pref

        if base_netloc == base_pref:
            netloc = pref.netloc.lower() # Force exact match to preference

    # Standardize path
    path = parsed.path if parsed.path else "/"
    
    # Remove multiple consecutive slashes (e.g. // -> /)
    while '//' in path:
        path = path.replace('//', '/')
        
    # Strip trailing slash to avoid duplicate fetches
    path = path.rstrip("/")
    if not path:
        path = "/"
        
    query = parsed.query

    return urlunparse((
        scheme,
        netloc,
        path,
        "",
        query,
        ""
    ))


# ============================================================
# CANONICAL DB ID (NO SCHEME)
# ============================================================

def get_canonical_id(url: str, base_url: str | None = None) -> str:
    """
    Returns a stable DB identifier: 'domain/path?query'

    - Removes scheme
    - Normalizes domain using base_url if provided
    - Preserves path + query
    """

    if not url:
        return ""

    # First normalize fully
    url = normalize_url(url, preference_url=base_url)
    parsed = urlparse(url)

    netloc = parsed.netloc.lower()

    # Enforce base_url domain if equivalent
    if base_url:
        base_parsed = urlparse(
            normalize_url(base_url)
        )

        clean_netloc = netloc[4:] if netloc.startswith("www.") else netloc
        clean_base = (
            base_parsed.netloc.lower()[4:]
            if base_parsed.netloc.lower().startswith("www.")
            else base_parsed.netloc.lower()
        )

        if clean_netloc == clean_base:
            netloc = base_parsed.netloc.lower()

    path = parsed.path.strip("/")
    query = f"?{parsed.query}" if parsed.query else ""

    if path:
        return f"{netloc}/{path}{query}"
    else:
        return f"{netloc}{query}"


# ============================================================
# HTML NORMALIZATION (HASHING / DIFF ONLY)
# ============================================================

def normalize_html(html: str) -> str:
    """
    Normalize HTML ONLY for hashing / comparison.
    Deterministic output.
    """
    if not html:
        return ""

    soup = BeautifulSoup(html, "lxml")

    # Remove noisy tags
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # Normalize whitespace
    normalized = soup.prettify()
    normalized = "\n".join(
        line.strip()
        for line in normalized.splitlines()
        if line.strip()
    )

    return normalized


# ============================================================
# JS RENDER NORMALIZATION (DISPLAY ONLY)
# ============================================================

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
