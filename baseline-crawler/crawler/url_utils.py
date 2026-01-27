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
    Ensures the URL netloc has a 'www.' prefix.
    Used for fetching to bypass non-www issues.
    
    🔒 SAFEGUARD: Only acts on naked domains (e.g. example.com).
    Does NOT touch existing subdomains (e.g. blog.example.com).
    """
    if not url: return ""
    p = urlparse(url)
    netloc = p.netloc.lower()
    
    if not netloc:
        return url

    # Extract port if present
    port = ""
    if ":" in netloc:
        netloc, port = netloc.split(":", 1)
        port = ":" + port

    # Heuristic: If it has exactly one dot and doesn't start with www, it's a naked domain
    # e.g. "hocco.in" (1 dot) -> prepend
    # e.g. "blog.hocco.in" (2 dots) -> skip
    # e.g. "www.hocco.in" (starts with www) -> skip
    if netloc.count(".") == 1 and not netloc.startswith("www."):
        netloc = "www." + netloc
    
    # Rebuild with port preserved
    return urlunparse((
        p.scheme,
        netloc + port,
        p.path,
        p.params,
        p.query,
        p.fragment
    ))
