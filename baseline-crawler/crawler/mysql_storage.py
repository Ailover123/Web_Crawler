import json
import hashlib
from typing import Optional
from crawler.models import CrawlResponse
from crawler.storage import ArtifactWriter

class MySQLArtifactStore(ArtifactWriter):
    """
    MySQL implementation of History Logging.
    Writes to 'crawl_history' table (Metadata Only).
    """

    def __init__(self, connection_pool):
        self._pool = connection_pool

    def write(self, response: CrawlResponse) -> None:
        """
        Log the crawl event. 
        Raw Body is NOT written.
        """
        # Generate Event ID: SHA256(session + url + attempt + timestamp)
        # Unique identifier for this specific network interaction
        uid_string = f"{response.session_id}:{response.normalized_url}:{response.attempt_number}:{response.request_timestamp}"
        event_id = hashlib.sha256(uid_string.encode('utf-8')).hexdigest()

        sql = """
            INSERT INTO crawl_history (
                event_id, session_id, normalized_url, attempt_number,
                http_status, content_type, response_headers, 
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE event_id=event_id
        """
        # Ensure headers are JSON serializable
        headers_json = json.dumps(response.response_headers)
        
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (
                event_id, response.session_id, response.normalized_url,
                response.attempt_number, response.http_status,
                response.content_type, headers_json, response.request_timestamp
            ))
            self._pool.commit()
