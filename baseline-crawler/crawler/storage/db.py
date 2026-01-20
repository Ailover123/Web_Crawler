"""
DB access layer
All DB access is routed through MySQL only
"""

from .mysql import (
    # connection
    get_connection,
    
    # existing writes
    insert_crawl_page,
    insert_baseline,
    insert_diff,

    # health + infra
    check_db_health,
    fetch_enabled_sites,

    # crawl job lifecycle
    insert_crawl_job,
    complete_crawl_job,
    fail_crawl_job,
)

# -------------------------------------------------
# Explicit exports
# -------------------------------------------------
__all__ = [
    # connection
    "get_connection",
    
    # crawl data
    "insert_crawl_page",
    "insert_baseline",
    "insert_diff",

    # site discovery
    "fetch_enabled_sites",

    # crawl job lifecycle
    "insert_crawl_job",
    "complete_crawl_job",
    "fail_crawl_job",

    # infra
    "check_db_health",
]
