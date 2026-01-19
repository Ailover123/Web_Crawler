-- Database Schema for Security Crawler
-- Authoritative Internal Storage Layer

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- 1. LEGACY COMPATIBILITY TABLES

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
    
    PRIMARY KEY (session_id, normalized_url(255)),
    FOREIGN KEY (session_id) REFERENCES crawl_sessions(session_id),
    INDEX idx_state_priority (session_id, state, priority),
    -- COVERING INDEX: Optimized for heartbeat recovery/crash detection
    INDEX idx_session_state_heartbeat (session_id, state, last_heartbeat)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 4. ARTIFACTS 
-- Raw Body is NOT stored here. This is purely metadata logging.

CREATE TABLE IF NOT EXISTS crawl_history (
    event_id CHAR(64) PRIMARY KEY,        -- Unique Event ID
    session_id CHAR(36) NOT NULL,
    normalized_url VARCHAR(2048) NOT NULL,
    attempt_number INT NOT NULL,
    http_status INT NOT NULL,
    content_type VARCHAR(255),
    response_headers JSON,
    
    -- LINK TO NORMALIZED CONTENT (Nullable if no change/no match yet)
    page_version_id CHAR(32) NULL, 
    
    created_at TIMESTAMP(3) NOT NULL,
    
    INDEX idx_session_url (session_id, normalized_url(255)),
    FOREIGN KEY (session_id) REFERENCES crawl_sessions(session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 5. NORMALIZED STORAGE (PHASE 2)
-- THE SINGLE SOURCE OF TRUTH FOR CONTENT

CREATE TABLE IF NOT EXISTS page_versions (
    page_version_id CHAR(32) PRIMARY KEY,       -- Hash of (url + content_hash + version)
    url_hash CHAR(64) NOT NULL,                 -- SHA256(normalized_url) for fast lookups
    content_hash CHAR(64) NOT NULL,             -- SHA256(normalized_text) for dedup
    
    -- METADATA
    title VARCHAR(512),
    normalized_text MEDIUMTEXT,                 -- The Clean Content
    normalization_version VARCHAR(10) DEFAULT 'v1',
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    
    INDEX idx_url_hash (url_hash),
    INDEX idx_content_hash (content_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 6. BASELINES (PHASE 4)
-- Refactored to point to page_versions

CREATE TABLE IF NOT EXISTS site_baselines (
    baseline_id CHAR(64) PRIMARY KEY,
    site_id INT NOT NULL,
    
    -- Pointer to the "Good" Version
    page_version_id CHAR(32) NOT NULL,
    
    is_active BOOLEAN NOT NULL DEFAULT 0,
    promoted_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    
    FOREIGN KEY (site_id) REFERENCES sites(siteid),
    FOREIGN KEY (page_version_id) REFERENCES page_versions(page_version_id),
    INDEX idx_site_active (site_id, is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 7. DETECTION (PHASE 5)

CREATE TABLE IF NOT EXISTS detection_verdicts (
    verdict_id CHAR(64) PRIMARY KEY,
    session_id CHAR(36) NOT NULL,
    
    -- Context
    url_hash CHAR(64) NOT NULL,                 -- SHA256(normalized_url)
    
    -- The Comparison
    previous_baseline_version_id CHAR(32) NOT NULL,
    current_page_version_id CHAR(32) NOT NULL,
    
    -- The Verdict
    status ENUM('CLEAN', 'POTENTIAL_DEFACEMENT', 'DEFACED', 'FAILED') NOT NULL,
    severity ENUM('NONE', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL') NOT NULL,
    confidence FLOAT NOT NULL,
    structural_drift FLOAT NOT NULL,
    content_drift FLOAT NOT NULL,
    detected_indicators JSON,
    
    analysis_timestamp TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    FOREIGN KEY (session_id) REFERENCES crawl_sessions(session_id),
    FOREIGN KEY (previous_baseline_version_id) REFERENCES page_versions(page_version_id),
    FOREIGN KEY (current_page_version_id) REFERENCES page_versions(page_version_id),
    INDEX idx_session (session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET FOREIGN_KEY_CHECKS = 1;
