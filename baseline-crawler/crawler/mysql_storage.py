import json
from typing import Optional
from crawler.models import CrawlArtifact
from crawler.storage import ArtifactWriter

class MySQLArtifactStore(ArtifactWriter):
    """
    MySQL implementation of ArtifactWriter.
    Stores raw network captures (Phase 3).
    """

    def __init__(self, connection_pool):
        self._pool = connection_pool

    def write(self, artifact: CrawlArtifact) -> None:
        """Atomically write a CrawlArtifact."""
        sql = """
            INSERT INTO crawl_artifacts (
                artifact_id, session_id, normalized_url, attempt_number,
                raw_body, http_status, content_type, response_headers, 
                request_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE artifact_id=artifact_id
        """
        # Ensure headers are JSON serializable
        headers_json = json.dumps(artifact.response_headers)
        
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (
                artifact.artifact_id, artifact.session_id, artifact.normalized_url,
                artifact.attempt_number, artifact.raw_body, artifact.http_status,
                artifact.content_type, headers_json, artifact.request_timestamp
            ))
            self._pool.commit()
