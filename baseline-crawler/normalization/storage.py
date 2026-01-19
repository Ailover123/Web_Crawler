from abc import ABC, abstractmethod
from typing import Optional
from normalization.models import PageVersion

class PageVersionStore(ABC):
    @abstractmethod
    def save(self, version: PageVersion) -> None:
        pass
    
    @abstractmethod
    def get_by_id(self, page_version_id: str) -> Optional[PageVersion]:
        pass

class MySQLPageVersionStore(PageVersionStore):
    def __init__(self, connection_pool):
        self._pool = connection_pool
        
    def save(self, version: PageVersion) -> None:
        """
        Idempotent Save: If version exists, we do nothing (IGNORE).
        """
        sql = """
            INSERT IGNORE INTO page_versions (
                page_version_id, url_hash, content_hash, 
                title, normalized_text, normalization_version, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (
                version.page_version_id, version.url_hash, version.content_hash,
                version.title, version.normalized_text, version.normalization_version,
                version.created_at
            ))
            self._pool.commit()

    def get_by_id(self, page_version_id: str) -> Optional[PageVersion]:
        sql = """
            SELECT page_version_id, url_hash, content_hash, title, 
                   normalized_text, normalization_version, created_at
            FROM page_versions WHERE page_version_id = %s
        """
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (page_version_id,))
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
                normalized_url="" # Not stored in versions table directly, usually context-dependent
            )
