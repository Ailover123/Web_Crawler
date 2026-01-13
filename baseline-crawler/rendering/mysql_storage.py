import json
from typing import Optional, List
from rendering.models import RenderedArtifact, RenderStatus
from rendering.storage import RenderedArtifactStore

class MySQLRenderedArtifactStore(RenderedArtifactStore):
    """
    MySQL implementation of RenderedArtifactStore.
    Stores post-JS enrichment results (Phase 6).
    """

    def __init__(self, connection_pool):
        self._pool = connection_pool

    def save(self, artifact: RenderedArtifact) -> None:
        """Atomically persist a RenderedArtifact."""
        sql = """
            INSERT INTO rendered_artifacts (
                rendered_artifact_id, artifact_id, rendered_body,
                render_status, render_timestamp, js_error_log
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE rendered_artifact_id=rendered_artifact_id
        """
        # Ensure error log is JSON
        error_log_json = json.dumps(artifact.js_error_log)
        
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (
                artifact.rendered_artifact_id, artifact.artifact_id,
                artifact.rendered_body, artifact.status.value,
                artifact.render_timestamp, error_log_json
            ))
            self._pool.commit()

    def get_by_parent(self, artifact_id: str) -> Optional[RenderedArtifact]:
        """Retrieve the rendered enrichment for a specific raw crawl artifact."""
        sql = """
            SELECT rendered_artifact_id, artifact_id, rendered_body, 
                   render_status, render_timestamp, js_error_log
            FROM rendered_artifacts
            WHERE artifact_id = %s
        """
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (artifact_id,))
            row = cursor.fetchone()
            if row:
                return RenderedArtifact(
                    rendered_artifact_id=row[0],
                    artifact_id=row[1],
                    rendered_body=row[2],
                    status=RenderStatus(row[3]),
                    render_timestamp=row[4],
                    js_error_log=json.loads(row[5]) if row[5] else []
                )
        return None
