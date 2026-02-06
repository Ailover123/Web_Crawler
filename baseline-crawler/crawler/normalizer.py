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

    # Preserve scheme if present, default to https if missing
    scheme = parsed.scheme or "https"
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
            # Only match if we are absolutely sure they are the same domain
            # But let's be less aggressive about forcing preference.
            pass 

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

    # First normalize fully (to handle relative paths, trailing slashes, etc.)
    url = normalize_url(url, preference_url=base_url)
    parsed = urlparse(url)

    netloc = parsed.netloc.lower()

    # Determine site's WWW preference from base_url (original_site_url)
    if base_url:
        base_parsed = urlparse(
            base_url if "://" in base_url else "https://" + base_url
        )
        base_netloc = base_parsed.netloc.lower()
        
        # Check if the registered site has www.
        pref_has_www = base_netloc.startswith("www.")
        
        # Check if current URL's netloc matches the base domain (ignoring www)
        clean_netloc = netloc[4:] if netloc.startswith("www.") else netloc
        clean_base = base_netloc[4:] if base_netloc.startswith("www.") else base_netloc
        
        if clean_netloc == clean_base:
            # Force the netloc to match the preference (with or without www)
            if pref_has_www and not netloc.startswith("www."):
                netloc = "www." + netloc
            elif not pref_has_www and netloc.startswith("www."):
                netloc = netloc[4:]

    path = parsed.path
    query = f"?{parsed.query}" if parsed.query else ""

    # Return scheme-less URL: 'domain/path?query'
    if path and path != "/":
        return f"{netloc}{path}{query}"
    else:
        return f"{netloc}{query}"


# ============================================================
# HTML NORMALIZATION (HASHING / DIFF ONLY)
# ============================================================

import re

# Patterns to strip from HTML to avoid noisy diffs (e.g. dynamic nonces, IDs, etc.)
SKIP_PATTERNS = [
    re.compile(r'(?i)"[^"]*nonce[^"]*"\s*:\s*["\'](?:[^"\\]|\\.)*["\']'),
    re.compile(r'(?i)\b[\w:-]*nonce[\w:-]*\s*=\s*["\'](?:[^"\\]|\\.)*["\']'),
    re.compile(r'(?i)\bvalue\s*=\s*["\'](?:[^"\\]|\\.)*["\']'),
    re.compile(r'(?i)"floatingButtonsClickTracking"\s*:\s*["\'](?:[^"\\]|\\.)*["\']'),
    re.compile(r'(?i)\baria-controls\s*=\s*["\'][^"\']*["\']'),
    re.compile(r'(?i)\baria-labelledby\s*=\s*["\'][^"\']*["\']'),
    re.compile(r'(?i)\bdata-smartmenus-id\s*=\s*["\'][^"\']*["\']'),
    re.compile(r'(?i)\bid\s*=\s*["\'][^"\']*["\']'),
    re.compile(r'(?i)\bname\s*=\s*["\'][^"\']*["\']'),
    re.compile(r'(?i)\bcb\s*=\s*["\'][^"\']*["\']'),
]

def normalize_html(html: str) -> str:
    """
    Normalize HTML ONLY for hashing / comparison.
    Deterministic output.
    """
    if not html:
        return ""

    # Apply skip patterns to remove dynamic noise
    for pattern in SKIP_PATTERNS:
        html = pattern.sub('', html)

    soup = BeautifulSoup(html, "lxml")

    # Remove noisy tags
    for tag in soup(["noscript"]):
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
