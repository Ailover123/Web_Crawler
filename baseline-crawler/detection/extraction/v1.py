import re
import hashlib
from typing import Dict, Any, List
from collections import Counter

def distill_v1_features(html: str) -> Dict[str, Any]:
    """
    Pinned Version 1 Extraction.
    Returns both structural tag counts and content features.
    """
    # 1. Structural tags
    # Remove script and style blocks
    html_cleaned = re.sub(r'<script.*?>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html_cleaned = re.sub(r'<style.*?>.*?</style>', '', html_cleaned, flags=re.DOTALL | re.IGNORECASE)
    html_cleaned = re.sub(r'<!--.*?-->', '', html_cleaned, flags=re.DOTALL)
    
    tags = re.findall(r'<([a-zA-Z0-9]+).*?>', html_cleaned)
    structural_features = dict(Counter(tags))
    structural_digest = hashlib.sha256("".join(tags).encode('utf-8')).hexdigest()

    # 2. Key-value features
    content_features = {}
    title_match = re.search(r'<title>(.*?)</title>', html, flags=re.IGNORECASE | re.DOTALL)
    content_features["title"] = title_match.group(1).strip() if title_match else None
    
    meta_desc = re.search(r'<meta name="description" content="(.*?)"', html, flags=re.IGNORECASE)
    content_features["meta_description"] = meta_desc.group(1) if meta_desc else None
    
    content_features["approx_size"] = len(html)

    return {
        "structural_digest": structural_digest,
        "structural_features": structural_features,
        "content_features": content_features
    }
