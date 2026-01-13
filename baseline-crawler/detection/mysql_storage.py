import json
from typing import Optional
from detection.models import DetectionVerdict, DetectionStatus, DetectionSeverity
from detection.storage import DetectionVerdictStore

class MySQLDetectionVerdictStore(DetectionVerdictStore):
    """
    MySQL implementation of DetectionVerdictStore.
    Stores immutable analysis results (Phase 5).
    """

    def __init__(self, connection_pool):
        self._pool = connection_pool

    def save(self, verdict: DetectionVerdict) -> None:
        """Atomically persist a verdict."""
        sql = """
            INSERT INTO detection_verdicts (
                verdict_id, session_id, artifact_id, baseline_id,
                status, severity, confidence, structural_drift,
                content_drift, detected_indicators, analysis_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE verdict_id=verdict_id
        """
        indicators_json = json.dumps(verdict.detected_indicators)
        
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (
                verdict.verdict_id, verdict.session_id, verdict.artifact_id, 
                verdict.baseline_id, verdict.status.value, verdict.severity.value,
                verdict.confidence, verdict.structural_drift,
                verdict.content_drift, indicators_json, verdict.analysis_timestamp
            ))
            self._pool.commit()

    def get_latest(self, normalized_url: str) -> Optional[DetectionVerdict]:
        """
        Retrieve the most recent analysis result for a URL by joining with crawl_artifacts.
        """
        sql = """
            SELECT v.verdict_id, v.session_id, v.artifact_id, v.baseline_id,
                   v.status, v.severity, v.confidence, v.structural_drift,
                   v.content_drift, v.detected_indicators, v.analysis_timestamp
            FROM detection_verdicts v
            JOIN crawl_artifacts a ON v.artifact_id = a.artifact_id
            WHERE a.normalized_url = %s
            ORDER BY v.analysis_timestamp DESC
            LIMIT 1
        """
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (normalized_url,))
            row = cursor.fetchone()
            if row:
                return DetectionVerdict(
                    verdict_id=row[0],
                    session_id=row[1],
                    artifact_id=row[2],
                    baseline_id=row[3],
                    status=DetectionStatus(row[4]),
                    severity=DetectionSeverity(row[5]),
                    confidence=row[6],
                    structural_drift=row[7],
                    content_drift=row[8],
                    detected_indicators=json.loads(row[9]) if row[9] else [],
                    analysis_timestamp=row[10]
                )
        return None
