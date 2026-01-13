"""
Verification Scenario for Baseline Promotion Flow
"""

import unittest
from unittest.mock import MagicMock
from baseline.mysql_storage import MySQLBaselineStore

class TestBaselinePromotion(unittest.TestCase):
    def setUp(self):
        self.mock_pool = MagicMock()
        self.store = MySQLBaselineStore(self.mock_pool)
        self.siteid = 1
        self.baseline_id = "target_baseline_sha256"

    def test_successful_promotion(self):
        """Scenario: Target baseline exists and belongs to site."""
        mock_cursor = self.mock_pool.cursor.return_value.__enter__.return_value
        # Sequence: Validate (Fetch 1) -> Lock -> Deactivate -> Activate
        mock_cursor.fetchone.return_value = (self.baseline_id,)
        mock_cursor.execute.side_effect = [
            None, # SELECT validate
            None, # SELECT FOR UPDATE
            1,    # UPDATE is_active = 0
            1     # UPDATE is_active = 1
        ]

        self.store.promote_baseline(self.siteid, self.baseline_id, actor_id="admin_123")
        
        # Verify transaction sequence
        self.mock_pool.begin.assert_called_once()
        self.mock_pool.commit.assert_called_once()
        self.mock_pool.rollback.assert_not_called()

    def test_failed_promotion_wrong_site(self):
        """Scenario: Baseline does not exist or belongs to another site (caught by validation read)."""
        mock_cursor = self.mock_pool.cursor.return_value.__enter__.return_value
        # Sequence: Validate (Fetch None) -> Rollback
        mock_cursor.fetchone.return_value = None
        mock_cursor.execute.side_effect = [
            None, # SELECT validate
        ]

        with self.assertRaises(ValueError) as cm:
            self.store.promote_baseline(self.siteid, self.baseline_id)
        
        self.assertIn("not found for site", str(cm.exception))
        self.mock_pool.rollback.assert_called_once()
        self.mock_pool.commit.assert_not_called()

    def test_idempotency_safety(self):
        """
        Scenario: Promoting a baseline that is ALREADY active.
        MySQL returns 0 affected rows if the row content is unchanged.
        However, in our logic, we deactivate ALL for the site first, 
        making the activation step ALWAYS affect 1 row if it exists.
        Thus, the flow is natively idempotent for 'existing' baselines.
        """
        mock_cursor = self.mock_pool.cursor.return_value.__enter__.return_value
        # Sequence: Lock -> Deactivate (affects 1) -> Activate (affects 1)
        mock_cursor.execute.side_effect = [None, 1, 1]
        
        # First call
        self.store.promote_baseline(self.siteid, self.baseline_id)
        
        # Second call (same result)
        self.store.promote_baseline(self.siteid, self.baseline_id)
        
        self.assertEqual(self.mock_pool.commit.call_count, 2)

if __name__ == "__main__":
    unittest.main()
