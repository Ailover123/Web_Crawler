import json
from typing import Optional
from baseline.models import BaselineProfile
from baseline.storage import BaselineStore

class MySQLBaselineStore(BaselineStore):
    """
    MySQL implementation of BaselineStore.
    Supports active baseline selection and profile storage.
    """

    def __init__(self, connection_pool):
        self._pool = connection_pool

    def get_active_baseline_id(self, site_id: int) -> Optional[str]:
        """Retrieve the ID of the currently ACTIVE baseline for a site."""
        sql = """
            SELECT baseline_id FROM site_baselines
            WHERE site_id = %s AND is_active = 1
            LIMIT 1
        """
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (site_id,))
            row = cursor.fetchone()
            return row[0] if row else None

    def save_profile(self, profile: BaselineProfile) -> None:
        """Atomically persist a profile."""
        sql = """
            INSERT INTO baseline_profiles (
                profile_id, baseline_id, normalized_url,
                structural_digest, structural_features, content_features
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE profile_id=profile_id
        """
        struct_features_json = json.dumps(profile.structural_features)
        content_features_json = json.dumps(profile.content_features)
        
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (
                profile.profile_id, profile.baseline_id, profile.normalized_url,
                profile.structural_digest, struct_features_json, content_features_json
            ))
            self._pool.commit()

    def get_profile(self, baseline_id: str, normalized_url: str) -> Optional[BaselineProfile]:
        """Retrieve a specific profile."""
        sql = """
            SELECT profile_id, baseline_id, normalized_url, 
                   structural_digest, structural_features, content_features
            FROM baseline_profiles
            WHERE baseline_id = %s AND normalized_url = %s
        """
        with self._pool.cursor() as cursor:
            cursor.execute(sql, (baseline_id, normalized_url))
            row = cursor.fetchone()
            if row:
                return BaselineProfile(
                    profile_id=row[0],
                    baseline_id=row[1],
                    normalized_url=row[2],
                    structural_digest=row[3],
                    structural_features=json.loads(row[4]) if row[4] else {},
                    content_features=json.loads(row[5]) if row[5] else {}
                )
        return None
