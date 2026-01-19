import sys
import os
import uuid
import time
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse
import pymysql

# Inject the baseline-crawler directory into sys.path
# This ensures all sub-packages (crawler, frontier, baseline, detection) are resolvable.
sys.path.append(os.path.join(os.path.dirname(__file__), "baseline-crawler"))

# Component Imports - These must exist or the system won't run.
from crawler.config import DB_CONFIG, USER_AGENT, REQUEST_TIMEOUT, VERIFY_SSL_CERTIFICATE
from crawler.policy import URLPolicy
from crawler.worker import CrawlWorker
from crawler.mysql_storage import MySQLArtifactStore
from normalization.engine import NormalizationEngine
from normalization.storage import MySQLPageVersionStore
from normalization.engine import NormalizationEngine
from normalization.storage import MySQLPageVersionStore

from frontier.orchestrator import Frontier
from frontier.mysql_storage import MySQLTaskStore

from baseline.extractor import BaselineExtractor
from baseline.mysql_storage import MySQLBaselineStore

from detection.engine import DefacementDetector
from detection.mysql_storage import MySQLDetectionVerdictStore
from detection.models import DetectionVerdict, DetectionStatus, DetectionSeverity

def verify_schema_or_exit(connection):
    """
    Startup Guard: Verify all required database tables exist.
    If any table is missing, print clear error and exit cleanly.
    """
    required_tables = [
        'crawl_sessions',
        'task_store',
        'crawl_artifacts',
        'rendered_artifacts',
        'site_baselines',
        'baseline_profiles',
        'detection_verdicts'
    ]
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            existing_tables = {row[0] for row in cursor.fetchall()}
            
            missing_tables = [table for table in required_tables if table not in existing_tables]
            
            if missing_tables:
                print("\n" + "=" * 60)
                print("DATABASE NOT INITIALIZED")
                print("=" * 60)
                print("\nThe following required tables are missing:")
                for table in missing_tables:
                    print(f"  - {table}")
                print("\nPlease run the database initialization script:")
                print("  1. Open phpMyAdmin")
                print("  2. Select database 'crawlerdb'")
                print("  3. Go to 'Import' tab")
                print("  4. Choose file: database/init.sql")
                print("  5. Click 'Go'")
                print("\nSee DATABASE_SETUP.md for detailed instructions.")
                print("=" * 60 + "\n")
                sys.exit(1)
    except Exception as e:
        print(f"SCHEMA_VERIFICATION_ERROR: {e}")
        sys.exit(1)

class CrawlSessionManager:
    """
    Orchestrates the 6-phase security crawler pipeline.
    Respects strict architectural invariants for gating and state management.
    """
    def __init__(self, mode="detection"):
        self.mode = mode
        self.connection = self._get_db_connection()
        self.session_id = str(uuid.uuid4())
        self.start_time = time.time()
        
        # Initialize Stores (Phase-specific data persistence)
        self.task_store = MySQLTaskStore(self.connection, self.session_id)
        self.artifact_store = MySQLArtifactStore(self.connection)
        self.page_version_store = MySQLPageVersionStore(self.connection)
        self.baseline_store = MySQLBaselineStore(self.connection)
        self.detection_store = MySQLDetectionVerdictStore(self.connection)
        
        # Initialize Engines (Analytical and Execution logic)
        self.frontier = Frontier(self.task_store) # Uses defaults for retries/heartbeat
        self.worker = CrawlWorker(self.artifact_store)
        self.normalizer = NormalizationEngine()
        self.extractor = BaselineExtractor(extraction_version="v1")
        self.detector = DefacementDetector(extraction_version="v1")
        
        self.detector = DefacementDetector(extraction_version="v1")

    def _get_db_connection(self):
        """Creates connection using authoritative DB_CONFIG."""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            # Startup Guard: Verify schema completeness
            verify_schema_or_exit(conn)
            return conn
        except Exception as e:
            print(f"DATABASE_ERROR: Failed to connect to MySQL: {e}")
            sys.exit(1)

    def run(self):
        # 1. Initialization: Isolation Invariant
        URLPolicy.reset_stats()

        # 2. Selection: Load active site
        sites = self._get_active_sites()
        if not sites:
            print("SESSION_ABORT: No active sites found in 'sites' table.")
            return

        site = sites[0] # Execution restricted to first active site by convention
        
        # 2a. Policy Configuration: Strict Domain Scope
        try:
            site_domain = urlparse(site['url']).netloc
            URLPolicy.set_allowed_domain(site_domain)
            print(f"POLICY_CONFIG: Enforcing strict domain scope: {site_domain}")
        except Exception:
            print("POLICY_ERROR: Failed to extract domain from site URL")
            return
        
        # 2. State Resolution: Check existing baselines
        active_baseline_id = self.baseline_store.get_active_baseline_id(site['siteid'])
        has_any_baseline = self._check_baseline_exists(site['siteid'])
        
        baseline_state_log = "active" if active_baseline_id else ("inactive" if has_any_baseline else "none")

        # 3. Initialization: Create session record
        self._create_session_record(site['siteid'])

        # 4. Discovery: Initial Seed URL
        # Policy Invariant: URLPolicy.should_crawl MUST be enforced during discovery
        if URLPolicy.should_crawl(site['url']):
            self.frontier.discover([site['url']], depth=0)

        # 5. Core Pipeline Execution
        while True:
            # Phase 2: Frontier Assignment
            task = self.frontier.assign_next()
            
            if not task:
                break
            
            # Phase 3: Crawl Execution
            # Worker consumes REQUEST_TIMEOUT, USER_AGENT, VERIFY_SSL_CERTIFICATE from config
            # Phase 1: Crawl & Fetch (Memory Only)
            crawl_task = self.frontier.prepare_crawl_task(task)
            response, extracted_urls = self.worker.execute(crawl_task)
            
            # Phase 2: Normalization (The Gatekeeper)
            # Converts raw response -> clean PageVersion
            page_version = self.normalizer.normalize(response)
            page_version_id = None
            
            if page_version:
                # Save Normalized Content (Idempotent)
                self.page_version_store.save(page_version)
                page_version_id = page_version.page_version_id

            # Phase 3: Hash & History Log (Refactored)
            # Log the event, linking to the content if it exists.
            self.artifact_store.write(response, page_version_id)
            
            # Artifact Retrieval Check
            if response and response.http_status > 0:
                self.frontier.report_success(task.normalized_url)
                
                # INVARIANT:
                # A successful crawl MUST attempt to discover child URLs.
                # Without discovery feedback, crawl degenerates to single-page fetch.
                for url in extracted_urls:
                    if URLPolicy.should_crawl(url):
                        self.frontier.discover([url], depth=task.depth + 1)
                
                # Phase 3b: Legacy Persistence (crawled_urls table)
                # Enforce: "child paths only", skip homepage
                self._persist_legacy_crawled_url(
                    site_id=site['siteid'],
                    full_url=task.normalized_url,
                    status=response.http_status,
                    depth=task.depth
                )

                # Phase 4: Baseline Generation (Gated Invariant)
                # Gate: Mode matches 'baseline' AND no existing baseline for site
                # In this mode, we TRUST the current content and promote it as truth.
                if self.mode == "baseline" and not has_any_baseline:
                    if page_version:
                        self.baseline_store.promote_baseline(site['siteid'], page_version)
                        # We do not verify promotion success; store raises if failed.
                
                # Phase 5: Defacement Detection (Gated Invariant)
                # Gate: Detection runs ONLY if an active baseline exists for the site.
                # If we are in 'baseline' mode but a baseline EXISTS, we still detect (safety).
                if has_any_baseline and page_version:
                    # 1. Fetch the Truth (Baseline Version for this URL)
                    baseline_version = self.baseline_store.get_baseline_version(site['siteid'], page_version.url_hash)
                    
                    verdict = None
                    if baseline_version:
                        # 2. Compare Present vs Truth
                        verdict = self.detector.analyze(
                            current=page_version, 
                            baseline=baseline_version, 
                            session_id=self.session_id
                        )
                    else:
                        # 3. Anomaly: URL exists now but was NOT in the baseline.
                        # Architectural Invariant: Treat as HIGH severity anomaly (New Page Injection).
                        # We create a pseudo-verdict for this structural violation.
                        
                        # We need to manually construct a Verdict since Detector expects two versions.
                        verdict = DetectionVerdict(
                            verdict_id=str(uuid.uuid4()),
                            session_id=self.session_id,
                            url_hash=page_version.url_hash,
                            previous_baseline_version_id="NULL",
                            current_page_version_id=page_version.page_version_id,
                            status=DetectionStatus.DEFACED,
                            severity=DetectionSeverity.HIGH,
                            confidence=1.0,
                            structural_drift=1.0,
                            content_drift=1.0,
                            detected_indicators=["UNEXPECTED_URL_INJECTION"],
                            analysis_timestamp=datetime.now(timezone.utc)
                        )
                    
                    if verdict:
                        self.detection_store.save(verdict)
            else:
                self.frontier.report_failure(task.normalized_url)

        # 6. Cleanup and Final Summary
        self._close_session_record()
        self._print_summary(site, baseline_state_log)

    def _get_active_sites(self):
        with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT siteid, url FROM sites WHERE is_active = 1")
            return cursor.fetchall() or []

    def _check_baseline_exists(self, site_id):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM site_baselines WHERE site_id = %s", (site_id,))
            return cursor.fetchone()[0] > 0

    def _ensure_baseline_record(self, site_id, baseline_id):
        # Creates an INACTIVE baseline record if it doesn't exist
        with self.connection.cursor() as cursor:
            cursor.execute(
                "INSERT IGNORE INTO site_baselines (baseline_id, site_id, version, is_active) VALUES (%s, %s, %s, %s)",
                (baseline_id, site_id, 1, 0)
            )
            self.connection.commit()

    def _create_session_record(self, site_id):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO crawl_sessions (session_id, site_id, status) VALUES (%s, %s, %s)",
                (self.session_id, site_id, 'RUNNING')
            )
            self.connection.commit()

    def _close_session_record(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "UPDATE crawl_sessions SET end_time = %s, status = 'COMPLETED' WHERE session_id = %s",
                (datetime.now(timezone.utc), self.session_id)
            )
            self.connection.commit()

    def _persist_legacy_crawled_url(self, site_id, full_url, status, depth):
        """
        Architectural Rule: Store CHILD PATH ONLY. 
        Homepage is NOT inserted.
        Normalization: No protocol, domain, leading/trailing slashes.
        """
        try:
            parsed = urlparse(full_url)
            # Normalization Rule: Strip protocol, domain, and surrounding slashes
            path = parsed.path.strip("/")
            
            # Homepage Handling: If path is empty, it's the root site. Do not insert.
            if not path:
                return

            sql = """
                INSERT INTO crawled_urls (siteid, url, http_status, crawl_depth, crawled_at)
                VALUES (%s, %s, %s, %s, %s)
            """
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (site_id, path, status, depth, datetime.now(timezone.utc)))
                self.connection.commit()
        except Exception as e:
            # Failure is logged but doesn't abort the session (best-effort legacy sink)
            print(f"LEGACY_STORAGE_ERROR: Failed to persist {full_url}: {e}")

    def _get_db_size_mb(self):
        """Calculates total storage used by the crawler database in MB."""
        try:
            with self.connection.cursor() as cursor:
                # Query information_schema for the specific database
                db_name = DB_CONFIG['db']
                sql = """
                    SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) 
                    FROM information_schema.TABLES 
                    WHERE table_schema = %s
                """
                cursor.execute(sql, (db_name,))
                size_mb = cursor.fetchone()[0]
                return size_mb if size_mb is not None else 0.0
        except Exception:
            return 0.0

    def _print_summary(self, site, baseline_state_log):
        """Unified Terminal Summary (Architecture Locked)"""
        duration = time.time() - self.start_time
        policy_stats = URLPolicy.get_stats()
        
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM detection_verdicts WHERE session_id = %s", (self.session_id,))
            total_verdicts = cursor.fetchone()[0]
            
        db_size_mb = self._get_db_size_mb()

        # Mandatory Key Verification: All policy keys MUST be displayed.
        print("\n==============================")
        print("CRAWL SESSION SUMMARY")
        print("==============================")
        print(f"Site:             {site['url']}")
        print(f"Duration:         {duration:.2f} seconds")
        print(f"URLs Evaluated:   {policy_stats.get('evaluations', 0)}")
        print(f"Allowed:          {policy_stats.get('allowed', 0)}")
        print(f"Blocked (breakdown):")
        print(f"  - blocked_non_http:      {policy_stats.get('blocked_non_http', 0)}")
        print(f"  - blocked_fragment:      {policy_stats.get('blocked_fragment', 0)}")
        print(f"  - blocked_asset:         {policy_stats.get('blocked_asset', 0)}")
        print(f"  - blocked_path_system:   {policy_stats.get('blocked_path_system', 0)}")
        print(f"  - blocked_path_taxonomy: {policy_stats.get('blocked_path_taxonomy', 0)}")
        print(f"  - blocked_substring:     {policy_stats.get('blocked_substring', 0)}")
        print(f"Baseline State:   {baseline_state_log}")
        print(f"Detection Verdicts: {total_verdicts}")
        print(f"Approx Artifacts: {policy_stats.get('allowed', 0)}")
        print(f"Total DB Usage:   {db_size_mb} MB (Real-time)")
        print("==============================\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Security Crawler CLI")
    parser.add_argument("--mode", choices=["baseline", "detection"], default="detection", help="Operation mode")
    args = parser.parse_args()

    manager = CrawlSessionManager(mode=args.mode)
    manager.run()
