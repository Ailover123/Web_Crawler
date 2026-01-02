# combined_domain_analysis.py
# Purpose: Generate domain distribution JSON for analysis.
# Phase: Analysis / Observability
# Output: domain_distribution.json
# Notes: Classifies URLs into types and generates distribution report.

from urllib.parse import urlparse
import json

def generate_combined_domain_analysis(frontier):
    """
    Generate domain distribution JSON for the single domain.
    """
    domains = set()
    for url in frontier.discovered:
        domain = urlparse(url).netloc
        domains.add(domain)

    # Assuming single domain
    domain = list(domains)[0] if domains else "unknown"
    urls_for_domain = [url for url in frontier.discovered if urlparse(url).netloc == domain]

    distribution = {}
    for url in urls_for_domain:
        url_type = frontier.classifications.get(normalize_url(url), "unknown")
        if isinstance(url_type, (set, list, tuple)):
            types_iter = list(url_type) if url_type else ["unknown"]
        else:
            types_iter = [url_type]

        for t in types_iter:
            if t not in distribution:
                distribution[t] = {"count": 0, "urls": []}
            distribution[t]["count"] += 1
            distribution[t]["urls"].append({"sr": len(distribution[t]["urls"]) + 1, "url": url})

    domain_distribution = {
        "domain": domain,
        "total_urls": len(urls_for_domain),
        "distribution": distribution
    }

    return domain_distribution

def normalize_url(url):
    """
    Normalize URL by removing trailing slash.
    """
    from urllib.parse import urlparse, urlunparse
    try:
        p = urlparse(url)
        path = p.path
        if path.endswith('/') and path != '/':
            path = path.rstrip('/')
        normalized = urlunparse((p.scheme, p.netloc, path, p.params, p.query, p.fragment))
        return normalized
    except Exception:
        return url
