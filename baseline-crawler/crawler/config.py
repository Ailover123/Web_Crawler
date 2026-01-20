from pathlib import Path

# Configuration for the web crawler.
# This file defines crawl scope, limits, and seed URLs.
# No depth logic, normalization, or defacement detection here.

# Initial URLs to start crawling from (MANDATORY)
SEED_URLS = [
     "https://worldpeoplesolutions.com/",
     "https://sitewall.net/",
    "https://uat.pagentra.com/"
]

# Network timeout for HTTP requests (seconds)
 # Increased to give slow pages more room before marking as timeout
REQUEST_TIMEOUT = 35

# User-Agent string for crawler identification
USER_AGENT = "Chrome/126.0.0.0"

# Canonical data directory for the crawler. Set to the `data` folder
# located inside the `baseline-crawler` package.
try:
    DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
except NameError:
    # Fallback to os.path (string) if Path is not defined for any reason
    import os
    DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# Worker scaling parameters
# IMPORTANT: MAX_WORKERS should NOT exceed available logical processors
# Your system: 6 physical cores, 12 logical processors (with hyperthreading)
# Optimal setting: 6 (matches physical cores, prevents CPU over-capacity)
MIN_WORKERS = 5
MAX_WORKERS = 6

# MySQL Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  
    'database': 'crawlerdb',
    'autocommit': False,
    'charset': 'utf8mb4',
    'raise_on_warnings': False,
    'connection_timeout': 10
}

# Centralized policy configuration
# - allow_taxonomy_paths: when True, blog/tag/category/author/date archive URLs are allowed

# ============================================================
# SITEID GENERATION LOGIC
# ============================================================
def generate_siteid(custid: int, site_number: int) -> int:
    """
    Generate siteid from custid and site_number.
    Format: custid as string + site_number with dynamic padding
    
    Examples:
      custid=123, site_number=1   → siteid=12301   (5 digits)
      custid=123, site_number=99  → siteid=12399   (5 digits)
      custid=123, site_number=100 → siteid=123100  (6 digits)
      custid=123, site_number=999 → siteid=123999  (6 digits)
    
    Args:
        custid: Customer ID (e.g., 123)
        site_number: Site number starting from 1 (e.g., 1, 2, 100)
    
    Returns:
        Combined siteid as integer
    """
    if site_number < 1:
        raise ValueError(f"site_number must be >= 1, got {site_number}")
    
    # Determine minimum padding width (at least 2 digits, more if site_number needs it)
    min_width = max(2, len(str(site_number)))
    
    # Format: custid_string + zero-padded site_number
    siteid_str = str(custid) + str(site_number).zfill(min_width)
    return int(siteid_str)
#   (feeds/search/wp-admin/wp-json/amp/print remain blocked)
POLICY = {
    'allow_taxonomy_paths': True,
}

# SSL Certificate Verification
# Set to False to allow crawling sites with invalid/self-signed SSL certificates
VERIFY_SSL_CERTIFICATE = False

