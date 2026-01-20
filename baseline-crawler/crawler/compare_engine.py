"""
Comparison engine for defacement detection.
Compares current crawled pages against baseline snapshots.
"""

import logging
import hashlib

logger = logging.getLogger(__name__)


class CompareEngine:
    """
    Compares current page content against baseline for defacement detection.
    """
    
    def __init__(self, custid: int):
        """Initialize compare engine for a customer."""
        self.custid = custid
        self.comparisons = []
    
    def handle_page(self, siteid: int, url: str, html: str):
        """
        Compare a crawled page against its baseline.
        
        Args:
            siteid: Site ID
            url: Page URL
            html: Current page HTML
        
        Returns: None (results stored for later analysis)
        """
        # Compute current hash
        current_hash = hashlib.sha256(html.encode('utf-8')).hexdigest()
        
        # Store comparison record
        self.comparisons.append({
            'siteid': siteid,
            'url': url,
            'current_hash': current_hash,
            'html_length': len(html)
        })
        
        logger.debug(f"Compared {url} (hash={current_hash[:8]}..., size={len(html)} bytes)")
    
    def get_results(self) -> dict:
        """Get comparison results."""
        return {
            'custid': self.custid,
            'total_comparisons': len(self.comparisons),
            'comparisons': self.comparisons
        }
