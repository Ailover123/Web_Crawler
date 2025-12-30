"""
URL extraction from HTML for the crawler.
Extracts and filters URLs to crawl.
"""

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def classify_url(url):
    """
    Classify a URL into zero or more types: pagination, uploads/assets, elementor, media, normal_html.
    Returns a set of types the URL belongs to.
    """
    types = set()
    url_lower = url.lower()
    path = urlparse(url).path.lower()

    # Pagination: common pagination patterns
    if any(pat in url_lower for pat in ['/page/', '/p/', '?page=', '?p=', '/pagination/']):
        types.add('pagination')

    # Uploads/assets: common upload directories
    if any(pat in url_lower for pat in ['/uploads/', '/assets/', '/wp-content/uploads/', '/media/', '/files/']):
        types.add('uploads/assets')

    # Elementor: specific to Elementor CMS
    if 'elementor' in url_lower:
        types.add('elementor')

    # Media: file extensions
    media_exts = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.svg']
    if any(path.endswith(ext) for ext in media_exts):
        types.add('media')

    # Normal HTML: if not classified above, assume it's normal HTML
    if not types:
        types.add('normal_html')

    return types

def extract_urls(html, base_url):
    """
    Extract URLs from HTML, filter to same domain, http/https, block certain types.
    Returns list of absolute URLs to crawl.
    """
    soup = BeautifulSoup(html, 'html.parser')
    base_domain = urlparse(base_url).netloc
    urls = []

    # Extract from <a href>
    for a in soup.find_all('a', href=True):
        url = urljoin(base_url, a['href'])
        if _is_allowed_url(url, base_domain):
            urls.append(url)

    return urls

def _is_allowed_url(url, base_domain):
    """
    Check if URL is allowed: same domain, http/https, not blocked extension.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        return False
    if parsed.netloc != base_domain:
        return False
    path = parsed.path.lower()
    blocked_exts = ['.css', '.js', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.pdf', '.zip', '.rar', '.exe', '.tar', '.gz', '.mp3', '.mp4', '.avi', '.mov', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']
    for ext in blocked_exts:
        if path.endswith(ext):
            return False
    return True
