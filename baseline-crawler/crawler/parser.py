# crawler/parser.py
# Purpose: URL extraction and classification from HTML for the crawler.
# Phase: Analysis / Observability
# Output: Extracted URLs and assets from HTML pages.
# Notes: Extracts navigational URLs and assets, classifies URLs into types.

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from crawler.policy import URLPolicy
from typing import Optional, Tuple, List
import tldextract
from crawler.frontier import _log_scope_rejection
from crawler.policy import URLPolicy

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

    # Assets/media/docs/scripts/styles: treat as assets
    if URLPolicy.is_asset(url):
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

def extract_urls(html, base_url, *, siteid: Optional[int] = None, site_root_url: Optional[str] = None):
    """
    Extract URLs from HTML, separating navigational links from assets.
    Assets (PDFs, images, media files) are NOT enqueued for crawling.
    Returns: (urls_to_crawl, asset_urls)
    """
    soup = BeautifulSoup(html, 'html.parser')
    base_domain = urlparse(base_url).netloc
    urls: List[str] = []
    assets: List[str] = []
    
    # Asset extensions that should never be crawled
    # Use centralized policy for asset detection

    # Extract from <a href>
    for a in soup.find_all('a', href=True):
        url = urljoin(base_url, a['href'])
        if _is_allowed_url(url, base_domain, siteid=siteid, site_root_url=site_root_url):
            # Check if URL is an asset (by extension)
            path_lower = urlparse(url).path.lower()
            if URLPolicy.is_asset(url):
                assets.append(url)  # Asset link - store but don't crawl
            else:
                urls.append(url)  # Regular page - crawl it

    # Extract assets from <img src>
    for img in soup.find_all('img', src=True):
        asset_url = urljoin(base_url, img['src'])
        if _is_allowed_url(asset_url, base_domain, siteid=siteid, site_root_url=site_root_url):
            assets.append(asset_url)

    # Extract assets from <link rel="icon">
    for link in soup.find_all('link', rel='icon', href=True):
        asset_url = urljoin(base_url, link['href'])
        if _is_allowed_url(asset_url, base_domain, siteid=siteid, site_root_url=site_root_url):
            assets.append(asset_url)

    # Extract assets from <link rel="stylesheet">
    for link in soup.find_all('link', rel='stylesheet', href=True):
        asset_url = urljoin(base_url, link['href'])
        if _is_allowed_url(asset_url, base_domain, siteid=siteid, site_root_url=site_root_url):
            assets.append(asset_url)

    # Extract assets from <script src>
    for script in soup.find_all('script', src=True):
        asset_url = urljoin(base_url, script['src'])
        if _is_allowed_url(asset_url, base_domain, siteid=siteid, site_root_url=site_root_url):
            assets.append(asset_url)

    return urls, assets

def _is_allowed_url(url: str, base_domain: str, *, siteid: Optional[int] = None, site_root_url: Optional[str] = None) -> bool:
    """
    Check if URL is allowed for extraction using www exception logic.
    Allows: root domain + www subdomain only (matching frontier scope logic).
    Logs scope mismatches to file when URL otherwise passes policy.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False

    # Extract registrable domains and subdomains using tldextract
    base_extracted = tldextract.extract(f"http://{base_domain}")
    base_reg_domain = f"{base_extracted.domain}.{base_extracted.suffix}".lower()
    
    url_extracted = tldextract.extract(url)
    url_reg_domain = f"{url_extracted.domain}.{url_extracted.suffix}".lower()
    url_subdomain = url_extracted.subdomain.lower()
    
    # Allow if: same registrable domain AND subdomain is "" or "www"
    allowed_subdomains = {"", "www"}
    same_scope = (url_reg_domain == base_reg_domain) and (url_subdomain in allowed_subdomains)

    if not same_scope:
        # Only log when the URL is otherwise crawlable by policy
        allowed_by_policy = URLPolicy.should_crawl(url)
        if allowed_by_policy and siteid is not None and site_root_url:
            # Log to file instead of terminal
            _log_scope_rejection(siteid, site_root_url, url, parsed.netloc, "scope_mismatch")
        return False

    return True
