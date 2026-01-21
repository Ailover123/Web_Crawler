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

def normalize_url(url: str, *, base: str | None = None, preference_url: str | None = None) -> str:
    """
    Standard normalization for fetching. 
    If preference_url is provided, it forces the domain to match the preference
    if they are base-equivalent (e.g. www vs non-www).
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
    
    # Apply Branding Preference
    if preference_url:
        p_parsed = urlparse(preference_url if "://" in preference_url else "http://" + preference_url)
        p_netloc = p_parsed.netloc.lower()
        
        # Strip ports for comparison
        clean_netloc = netloc.split(":")[0]
        clean_pref = p_netloc.split(":")[0]
        
        # Basic equivalency check (e.g. sitewall.net vs www.sitewall.net)
        base_netloc = clean_netloc[4:] if clean_netloc.startswith("www.") else clean_netloc
        base_pref = clean_pref[4:] if clean_pref.startswith("www.") else clean_pref
        
        if base_netloc == base_pref:
            netloc = p_netloc # Force exact match to preference

    # Standardize: Strip trailing slash to avoid duplicate fetches
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
    
    # Home Page Skip: REMOVED to allow baseline storage of root domain
    # if not path and not query:
    #     return ""
    
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
