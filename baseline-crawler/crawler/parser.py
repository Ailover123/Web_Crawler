# crawler/parser.py
# Purpose: URL extraction and classification from HTML for the crawler.
# Phase: Analysis / Observability
# Output: Extracted URLs and assets from HTML pages.
# Notes: Extracts navigational URLs and assets, classifies URLs into types.

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

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

    # Extract from <a href>
    for a in soup.find_all('a', href=True):
        url = urljoin(base_url, a['href'])
        if _is_allowed_url(url, base_domain):
            urls.append(url)

    # Extract assets from <img src>
    for img in soup.find_all('img', src=True):
        asset_url = urljoin(base_url, img['src'])
        if _is_allowed_url(asset_url, base_domain):
            assets.append(asset_url)

    # Extract assets from <link rel="icon">
    for link in soup.find_all('link', rel='icon', href=True):
        asset_url = urljoin(base_url, link['href'])
        if _is_allowed_url(asset_url, base_domain):
            assets.append(asset_url)

    # Extract assets from <link rel="stylesheet">
    for link in soup.find_all('link', rel='stylesheet', href=True):
        asset_url = urljoin(base_url, link['href'])
        if _is_allowed_url(asset_url, base_domain):
            assets.append(asset_url)

    # Extract assets from <script src>
    for script in soup.find_all('script', src=True):
        asset_url = urljoin(base_url, script['src'])
        if _is_allowed_url(asset_url, base_domain):
            assets.append(asset_url)

    return urls, assets

def _is_allowed_url(url, base_domain):
    """
    Check if URL is allowed: same domain, http/https.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        return False
    if parsed.netloc != base_domain:
        return False
    return True
