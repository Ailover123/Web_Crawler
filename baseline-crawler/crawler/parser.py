# crawler/parser.py
# Purpose: URL extraction and classification from HTML for the crawler.
# Phase: Analysis / Observability
# Output: Extracted URLs and assets from HTML pages.
# Notes: Extracts navigational URLs and assets, classifies URLs into types.

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse

def classify_url(url):
    """
    Classify a URL into zero or more types: normal_html, pagination, assets_uploads, media, scripts_styles, api_like, unknown.
    Returns a set of types the URL belongs to.
    """
    types = set()
    url_lower = url.lower()
    path = urlparse(url).path.lower()

    # Pagination: common pagination patterns
    if any(pat in url_lower for pat in ['/page/', '/p/', '?page=', '?p=', '/pagination/']):
        types.add('pagination')

    # Assets uploads: common upload directories
    if any(pat in url_lower for pat in ['/uploads/', '/assets/', '/wp-content/uploads/', '/media/', '/files/']):
        types.add('assets_uploads')

    # Media: file extensions (merged into assets)
    media_exts = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.svg']
    if any(path.endswith(ext) for ext in media_exts):
        types.add('assets_uploads')

    # Scripts styles: CSS and JS
    if path.endswith('.css') or path.endswith('.js'):
        types.add('scripts_styles')

    # API like: wp-json, api
    if 'wp-json' in url_lower or '/api/' in url_lower:
        types.add('api_like')

    # Normal HTML: if not classified above, assume it's normal HTML
    if not types:
        types.add('normal_html')

    return types

def extract_urls(html, base_url):
    """
    Extract URLs from HTML, including navigational and assets, filter to same domain, http/https.
    Returns list of absolute URLs to crawl and assets.
    """
    soup = BeautifulSoup(html, 'html.parser')
    base_domain = urlparse(base_url).netloc
    urls = []
    assets = []

    def strip_fragment(u: str) -> str:
        p = urlparse(u)
        # Drop the fragment to avoid "forceful hash" URLs like .../#section
        return urlunparse((p.scheme, p.netloc, p.path, p.params, p.query, ""))

    # Extract from <a href>
    for a in soup.find_all('a', href=True):
        href = a['href']
        # Skip pure fragment anchors (e.g., '#pricing')
        if href.strip().startswith('#'):
            continue
        url = strip_fragment(urljoin(base_url, href))
        if _is_allowed_url(url, base_domain):
            urls.append(url)

    # Extract assets from <img src>
    for img in soup.find_all('img', src=True):
        asset_url = strip_fragment(urljoin(base_url, img['src']))
        if _is_allowed_url(asset_url, base_domain):
            assets.append(asset_url)

    # Extract assets from <link rel="icon">
    for link in soup.find_all('link', rel='icon', href=True):
        asset_url = strip_fragment(urljoin(base_url, link['href']))
        if _is_allowed_url(asset_url, base_domain):
            assets.append(asset_url)

    # Extract assets from <link rel="stylesheet">
    for link in soup.find_all('link', rel='stylesheet', href=True):
        asset_url = strip_fragment(urljoin(base_url, link['href']))
        if _is_allowed_url(asset_url, base_domain):
            assets.append(asset_url)

    # Extract assets from <script src>
    for script in soup.find_all('script', src=True):
        asset_url = strip_fragment(urljoin(base_url, script['src']))
        if _is_allowed_url(asset_url, base_domain):
            assets.append(asset_url)

    return urls, assets

def _is_allowed_url(url, base_domain):
    """
    Allow only http/https and restrict to the same registrable host,
    treating `www.` and non-`www` as equivalent. This prevents the
    common case where the seed is `example.com` but links are
    `www.example.com` (or vice versa) from being excluded.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False

    # Normalize hosts by stripping ports and leading 'www.'
    def _canon_host(host: str) -> str:
        if not host:
            return ""
        h = host.lower().split(":")[0]
        return h[4:] if h.startswith("www.") else h

    cand = _canon_host(parsed.netloc)
    base = _canon_host(base_domain)

    return cand == base
