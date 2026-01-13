-- Database Schema for Security Crawler
-- Authoritative Internal Storage Layer

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- 1. LEGACY COMPATIBILITY TABLES
-- RESTORED: These schemas are exactly as in the old codebase.
-- DO NOT MODIFY COLUMNS, NAMES, OR TYPES.

CREATE TABLE IF NOT EXISTS sites (
    siteid INT AUTO_INCREMENT PRIMARY KEY,
    url VARCHAR(255) UNIQUE,
    app_type VARCHAR(50),
    custid INT,
    added_by VARCHAR(100),
    time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active TINYINT DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS crawled_urls (
    id INT AUTO_INCREMENT PRIMARY KEY,
    siteid INT,
    url VARCHAR(255),
    http_status INT,
    crawl_depth INT,
    crawled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 2. SESSION MANAGEMENT

CREATE TABLE IF NOT EXISTS crawl_sessions (
    session_id CHAR(36) PRIMARY KEY,
    site_id INT NOT NULL,
    start_time TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    end_time TIMESTAMP(3) NULL,
    status ENUM('INITIALIZING', 'RUNNING', 'COMPLETED', 'FAILED') NOT NULL,
    FOREIGN KEY (site_id) REFERENCES sites(siteid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 3. FRONTIER (PHASE 2)

CREATE TABLE IF NOT EXISTS task_store (
    session_id CHAR(36) NOT NULL,
    normalized_url VARCHAR(2048) NOT NULL,
    state ENUM('DISCOVERED', 'PENDING', 'ASSIGNED', 'COMPLETED', 'FAILED') NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    last_heartbeat TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    priority INT NOT NULL DEFAULT 0,
    depth INT NOT NULL DEFAULT 0,
    
    PRIMARY KEY (session_id, normalized_url),
    FOREIGN KEY (session_id) REFERENCES crawl_sessions(session_id),
    INDEX idx_state_priority (session_id, state, priority),
    -- COVERING INDEX: Optimized for heartbeat recovery/crash detection
    INDEX idx_session_state_heartbeat (session_id, state, last_heartbeat)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 4. ARTIFACTS (PHASES 3 & 6)

CREATE TABLE IF NOT EXISTS crawl_artifacts (
    artifact_id CHAR(64) PRIMARY KEY,
    session_id CHAR(36) NOT NULL,
    normalized_url VARCHAR(2048) NOT NULL,
    attempt_number INT NOT NULL,
    raw_body LONGBLOB,
    http_status INT NOT NULL,
    content_type VARCHAR(255),
    response_headers JSON,
    request_timestamp TIMESTAMP(3) NOT NULL,
    
    UNIQUE INDEX idx_session_url_attempt (session_id, normalized_url, attempt_number),
    FOREIGN KEY (session_id) REFERENCES crawl_sessions(session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- PHASE 6: Optional Enrichment
CREATE TABLE IF NOT EXISTS rendered_artifacts (
    rendered_artifact_id CHAR(64) PRIMARY KEY,
    artifact_id CHAR(64) NOT NULL,
    rendered_body LONGTEXT,
    render_status ENUM('SUCCESS', 'TIMEOUT', 'FAILED') NOT NULL,
    js_error_log JSON,
    render_timestamp TIMESTAMP(3) NOT NULL,
    
    FOREIGN KEY (artifact_id) REFERENCES crawl_artifacts(artifact_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 5. BASELINES (PHASE 4)

CREATE TABLE IF NOT EXISTS site_baselines (
    baseline_id CHAR(64) PRIMARY KEY,
    site_id INT NOT NULL,
    version INT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT 0,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    
    FOREIGN KEY (site_id) REFERENCES sites(siteid),
    -- ENFORCEMENT: Only one active baseline per site
    UNIQUE INDEX idx_single_active_per_site (site_id, (CASE WHEN is_active = 1 THEN 1 ELSE NULL END))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS baseline_profiles (
    profile_id CHAR(64) PRIMARY KEY,
    baseline_id CHAR(64) NOT NULL,
    normalized_url VARCHAR(2048) NOT NULL,
    structural_digest CHAR(64) NOT NULL,
    structural_features JSON,
    content_features JSON,
    
    FOREIGN KEY (baseline_id) REFERENCES site_baselines(baseline_id),
    UNIQUE INDEX idx_baseline_url (baseline_id, normalized_url)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 6. DETECTION (PHASE 5)

CREATE TABLE IF NOT EXISTS detection_verdicts (
    verdict_id CHAR(64) PRIMARY KEY,
    session_id CHAR(36) NOT NULL,
    artifact_id CHAR(64) NOT NULL,
    baseline_id CHAR(64) NOT NULL,
    status ENUM('CLEAN', 'POTENTIAL_DEFACEMENT', 'DEFACED', 'FAILED') NOT NULL,
    severity ENUM('NONE', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL') NOT NULL,
    confidence FLOAT NOT NULL,
    structural_drift FLOAT NOT NULL,
    content_drift FLOAT NOT NULL,
    detected_indicators JSON,
    analysis_timestamp TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    FOREIGN KEY (session_id) REFERENCES crawl_sessions(session_id),
    FOREIGN KEY (artifact_id) REFERENCES crawl_artifacts(artifact_id),
    FOREIGN KEY (baseline_id) REFERENCES site_baselines(baseline_id),
    INDEX idx_session (session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET FOREIGN_KEY_CHECKS = 1;
