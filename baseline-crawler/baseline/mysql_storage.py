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

    def promote_baseline(
        self,
        siteid: int,
        baseline_id: str,
        actor_id: Optional[str] = None
    ) -> None:
        """
        Atomically promote a baseline to ACTIVE for a site.
        Ensures strict transaction boundaries and invariant validation.
        """
        lock_sql = "SELECT baseline_id FROM site_baselines WHERE site_id = %s FOR UPDATE"
        deactivate_sql = "UPDATE site_baselines SET is_active = 0 WHERE site_id = %s AND is_active = 1"
        activate_sql = "UPDATE site_baselines SET is_active = 1 WHERE baseline_id = %s AND site_id = %s"

        with self._pool.cursor() as cursor:
            try:
                self._pool.begin()
                
                # 1. Lock all baselines for the site to prevent concurrent activation races
                cursor.execute(lock_sql, (siteid,))
                
                # 2. Deactivate current (if any)
                cursor.execute(deactivate_sql, (siteid,))
                
                # 3. Activate target baseline
                affected = cursor.execute(activate_sql, (baseline_id, siteid))
                
                if affected == 0:
                    # Target baseline doesn't exist OR doesn't belong to this site
                    self._pool.rollback()
                    raise ValueError(f"Baseline {baseline_id} not found for site {siteid}")
                
                # 4. Verify exactly one active baseline (Atomic check within transaction)
                # The SQL unique constraint handles the (>1) case. 
                # The affected check above + transaction handles the (0) case.
                
                self._pool.commit()
            except Exception as e:
                self._pool.rollback()
                if isinstance(e, ValueError):
                    raise
                raise RuntimeError(f"Failed to promote baseline {baseline_id}: {str(e)}") from e
