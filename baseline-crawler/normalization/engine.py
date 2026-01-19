import re
import hashlib
from datetime import datetime
from collections import Counter
from typing import Dict, Any, Optional

from crawler.models import CrawlResponse
from normalization.models import PageVersion

class NormalizationEngine:
    """
    Phase 2 Logic: The Gatekeeper.
    Converts Raw CrawlResponse -> Normalized PageVersion.
    """
    
    def __init__(self, version: str = "v1"):
        self.version = version

    def normalize(self, response: CrawlResponse) -> Optional[PageVersion]:
        """
        Accepts a raw response, strips noise, and returns a PageVersion.
        Returns None if content is not suitable for normalization (e.g. empty).
        """
        if not response.raw_body:
            return None

        # 1. Decode (Best Effort)
        text_content = response.raw_body.decode('utf-8', errors='replace')
        
        # 2. Logic: Strip Noise (Based on v1 Rules)
        # Remove script and style blocks
        clean_text = re.sub(r'<script.*?>.*?</script>', '', text_content, flags=re.DOTALL | re.IGNORECASE)
        clean_text = re.sub(r'<style.*?>.*?</style>', '', clean_text, flags=re.DOTALL | re.IGNORECASE)
        clean_text = re.sub(r'<!--.*?-->', '', clean_text, flags=re.DOTALL)
        
        # Collapse Whitespace
        clean_text = " ".join(clean_text.split())
        
        # 3. Extract Metadata
        title_match = re.search(r'<title>(.*?)</title>', text_content, flags=re.IGNORECASE | re.DOTALL)
        title = title_match.group(1).strip() if title_match else None

        # 4. Compute Hashes
        # Content Hash: SHA256(clean_text)
        content_hash = hashlib.sha256(clean_text.encode('utf-8')).hexdigest()
        
        # URL Hash: SHA256(normalized_url) - For fast lookups
        url_hash = hashlib.sha256(response.normalized_url.encode('utf-8')).hexdigest()
        
        # Page Version ID: SHA256(url + content + version)
        # Deterministic ID for this specific version of this specific page
        pv_seed = f"{response.normalized_url}:{content_hash}:{self.version}"
        page_version_id = hashlib.sha256(pv_seed.encode('utf-8')).hexdigest()

        return PageVersion(
            page_version_id=page_version_id,
            url_hash=url_hash,
            content_hash=content_hash,
            title=title,
            normalized_text=clean_text,
            normalized_url=response.normalized_url,
            normalization_version=self.version
        )
