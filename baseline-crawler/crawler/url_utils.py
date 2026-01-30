import tldextract
from urllib.parse import urlparse, urlunparse

def canonicalize_seed(url: str) -> str:
    """
    Canonical form for seed URLs:
    - scheme + netloc preserved
    - NO trailing slash at root
    - no query / fragment
    """
    p = urlparse(url)

    # root path must be empty, not "/"
    path = p.path.rstrip("/")
    if path == "":
        path = ""

    return urlunparse((
        p.scheme,
        p.netloc,
        path,
        "", "", ""
    ))

def force_www_url(url: str) -> str:
    """
    Ensures the URL netloc has a 'www.' prefix if it's a naked domain.
    Uses tldextract for robust domain parsing (e.g. handles .co.uk correctly).
    
    🔒 SAFEGUARD: Only acts on naked domains (e.g. example.com).
    Does NOT touch existing subdomains (e.g. blog.example.com).
    """
    if not url: return ""
    
    # Parse with tldextract
    ext = tldextract.extract(url)
    
    # A naked domain has an empty subdomain but non-empty domain and suffix
    if not ext.subdomain and ext.domain and ext.suffix:
        p = urlparse(url)
        # Rebuild netloc with www. prefix
        # Preserve port if present in original netloc
        netloc = p.netloc.lower()
        port = ""
        if ":" in netloc:
            _, port = netloc.split(":", 1)
            port = ":" + port
            
        new_netloc = f"www.{ext.domain}.{ext.suffix}{port}"
        
        return urlunparse((
            p.scheme,
            new_netloc,
            p.path,
            p.params,
            p.query,
            p.fragment
        ))
    
    return url
