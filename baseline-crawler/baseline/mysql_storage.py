import hashlib
import uuid
from typing import Optional
from datetime import datetime
from baseline.models import SiteBaseline
from normalization.models import PageVersion

class MySQLBaselineStore:
    """
    Manages the 'Site Baseline' state.
    A Baseline is simply a pointer to a specific 'Valid' PageVersion.
    """
    def __init__(self, connection_pool):
        self._pool = connection_pool

    def promote_baseline(self, site_id: int, page_version: PageVersion) -> SiteBaseline:
        """
        Promotes a PageVersion to be the active baseline for this site/URL context.
        """
        # Generate Baseline ID: SHA256(site_id + normalized_url) -> One active baseline per URL per Site
        # We enforce one baseline per URL by using a deterministic ID.
        baseline_id = hashlib.sha256(f"{site_id}:{page_version.url_hash}".encode('utf-8')).hexdigest()
        
        sql = """
            INSERT INTO site_baselines (
                baseline_id, site_id, page_version_id, is_active, promoted_at
            ) VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                page_version_id = VALUES(page_version_id),
                promoted_at = VALUES(promoted_at),
                is_active = 1
        """
        
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (
                baseline_id, site_id, page_version.page_version_id, 
                1, datetime.utcnow()
            ))
            self._pool.commit()
            
        return SiteBaseline(
            baseline_id=baseline_id,
            site_id=site_id,
            page_version_id=page_version.page_version_id,
            is_active=True
        )

    def get_baseline_version(self, site_id: int, url_hash: str) -> Optional[PageVersion]:
        """
        Retrieves the 'Good' PageVersion for a given URL on this site.
        Joins site_baselines -> page_versions.
        """
        sql = """
            SELECT pv.page_version_id, pv.url_hash, pv.content_hash, 
                   pv.title, pv.normalized_text, pv.normalization_version, pv.created_at
            FROM site_baselines sb
            JOIN page_versions pv ON sb.page_version_id = pv.page_version_id
            WHERE sb.site_id = %s AND pv.url_hash = %s AND sb.is_active = 1
        """
        
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (site_id, url_hash))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return PageVersion(
                page_version_id=row[0],
                url_hash=row[1],
                content_hash=row[2],
                title=row[3],
                normalized_text=row[4],
                normalization_version=row[5],
                created_at=row[6],
                normalized_url="" # Not available in this join, but known by caller
            )
