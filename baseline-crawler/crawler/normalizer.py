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
    """
    Standard normalization for fetching. 
    Ensures scheme and domain are consistent but KEEPS them for valid HTTP requests.
    """
    if not url:
        return ""

    url = url.strip()

    # Pre-check: if it has no scheme, but looks like a domain, prep it
    if "://" not in url and not url.startswith("/"):
        url = "http://" + url

    if base:
        url = urljoin(base, url)

    parsed = urlparse(url)

    scheme = parsed.scheme.lower() if parsed.scheme else "http"
    netloc = parsed.netloc.lower()
    
    # Standardize: Strip trailing slash to avoid duplicate fetches
    # of "example.com/about/" and "example.com/about"
    path = (parsed.path or "/").rstrip("/")
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


def get_canonical_id(url: str, base_url: str | None = None) -> str:
    """
    Returns a CLEAN "Domain/Path" string for Database storage.
    - Strips 'http://' and 'https://'
    - Keeps the domain (netloc)
    - If base_url is provided, it uses the base_url's domain to ensure consistency (matching sites table).
    - Strips leading/trailing slashes from the path.
    - Returns empty string for the home page (root) to skip redundant storage.
    
    Example (base=https://sitewall.net): 'https://www.sitewall.net/about/' -> 'sitewall.net/about'
    Example (base=https://www.sitewall.net): 'https://sitewall.net/about/' -> 'www.sitewall.net/about'
    """
    if not url:
        return ""
    
    # Standardize current URL
    url = normalize_url(url)
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    
    # If a base URL is provided, we prefer its netloc formatting (matching the sites table)
    if base_url:
        base_parsed = urlparse(normalize_url(base_url))
        base_netloc = base_parsed.netloc.lower()
        
        # Only swap if they are "base-equivalent" (one is a www-version of the other)
        # to avoid accidentally mapping external domains to our site
        clean_netloc = netloc[4:] if netloc.startswith("www.") else netloc
        clean_base = base_netloc[4:] if base_netloc.startswith("www.") else base_netloc
        
        if clean_netloc == clean_base:
            netloc = base_netloc

    path = (parsed.path or "").strip("/")
    query = f"?{parsed.query}" if parsed.query else ""
    
    # Home Page Skip: If it's just the root domain with no path or query
    if not path and not query:
        return ""
    
    return f"{netloc}/{path}{query}".strip("/")


# -------------------------
# HTML NORMALIZATION (FOR HASHING / DIFF)
# -------------------------

def normalize_html(html: str) -> str:
    """
    Normalize HTML ONLY for hashing & comparison.
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
