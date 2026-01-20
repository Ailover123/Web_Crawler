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
