-- Database Initialization Script for Web Crawler
-- Creates all required tables for baseline-crawler

-- ============================================================
-- SITES TABLE - Customer domains to monitor
-- ============================================================
CREATE TABLE IF NOT EXISTS `sites` (
  `siteid` INT AUTO_INCREMENT PRIMARY KEY,
  `custid` INT NOT NULL,
  `url` VARCHAR(500) NOT NULL UNIQUE,
  `enabled` TINYINT(1) DEFAULT 1,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_custid` (`custid`),
  INDEX `idx_enabled` (`enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- CRAWL_JOBS TABLE - Lifecycle tracking per crawl run
-- ============================================================
CREATE TABLE IF NOT EXISTS `crawl_jobs` (
  `job_id` VARCHAR(36) PRIMARY KEY,
  `custid` INT NOT NULL,
  `siteid` INT NOT NULL,
  `start_url` VARCHAR(500) NOT NULL,
  `status` ENUM('running', 'completed', 'failed') DEFAULT 'running',
  `pages_crawled` INT DEFAULT 0,
  `started_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `completed_at` TIMESTAMP NULL,
  `error_msg` TEXT,
  FOREIGN KEY (`siteid`) REFERENCES `sites` (`siteid`) ON DELETE CASCADE,
  INDEX `idx_custid` (`custid`),
  INDEX `idx_siteid` (`siteid`),
  INDEX `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- CRAWL_PAGES TABLE - Individual pages fetched per job
-- ============================================================
CREATE TABLE IF NOT EXISTS `crawl_pages` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `job_id` VARCHAR(36) NOT NULL,
  `custid` INT NOT NULL,
  `siteid` INT NOT NULL,
  `url` VARCHAR(500) NOT NULL,
  `parent_url` VARCHAR(500),
  `status_code` INT,
  `content_type` VARCHAR(100),
  `content_length` BIGINT,
  `response_time_ms` INT,
  `fetched_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (`job_id`) REFERENCES `crawl_jobs` (`job_id`) ON DELETE CASCADE,
  FOREIGN KEY (`siteid`) REFERENCES `sites` (`siteid`) ON DELETE CASCADE,
  INDEX `idx_job_id` (`job_id`),
  INDEX `idx_url` (`url`),
  INDEX `idx_siteid` (`siteid`),
  UNIQUE KEY `unique_page_per_job` (`job_id`, `url`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- BASELINES TABLE - Stored baseline snapshots
-- ============================================================
CREATE TABLE IF NOT EXISTS `baselines` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `siteid` INT NOT NULL,
  `url` VARCHAR(500) NOT NULL,
  `html_hash` VARCHAR(64) NOT NULL,
  `snapshot_path` VARCHAR(500),
  `script_sources` JSON,
  `baseline_created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `baseline_updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (`siteid`) REFERENCES `sites` (`siteid`) ON DELETE CASCADE,
  INDEX `idx_siteid` (`siteid`),
  INDEX `idx_url` (`url`),
  INDEX `idx_hash` (`html_hash`),
  UNIQUE KEY `unique_baseline` (`siteid`, `url`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- DIFF_EVIDENCE TABLE - Defacement detection results
-- ============================================================
CREATE TABLE IF NOT EXISTS `diff_evidence` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `siteid` INT NOT NULL,
  `url` VARCHAR(500) NOT NULL,
  `baseline_hash` VARCHAR(64),
  `observed_hash` VARCHAR(64),
  `diff_summary` JSON,
  `severity` ENUM('NONE', 'LOW', 'MEDIUM', 'HIGH') DEFAULT 'MEDIUM',
  `status` ENUM('open', 'closed', 'acknowledged') DEFAULT 'open',
  `detected_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `closed_at` TIMESTAMP NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (`siteid`) REFERENCES `sites` (`siteid`) ON DELETE CASCADE,
  INDEX `idx_siteid` (`siteid`),
  INDEX `idx_url` (`url`),
  INDEX `idx_status` (`status`),
  INDEX `idx_severity` (`severity`),
  INDEX `idx_detected_at` (`detected_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- Sample Data
-- ============================================================
-- Insert a sample customer site
INSERT IGNORE INTO `sites` (`custid`, `url`, `enabled`) VALUES 
(101, 'https://worldpeoplesolutions.com', 1),
(101, 'https://example.com', 1);

-- Display confirmation
SELECT 'Database initialization complete!' AS status;
