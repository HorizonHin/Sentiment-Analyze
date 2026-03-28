-- MSSQL Topic snapshot schema
-- Design goals:
-- 1) Topic table uses (created_at, id) composite primary key.
-- 2) topic is intentionally NOT unique.
-- 3) history table stores metric snapshots for trend calculation.
-- 4) Indexes follow leftmost-prefix pattern for common queries.
use sentiment_analyze;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Topic')
CREATE TABLE Topic (
    created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    id BIGINT IDENTITY(1,1) NOT NULL,
    topic NVARCHAR(300) NOT NULL,
    platform_distribution_json NVARCHAR(MAX) NOT NULL DEFAULT '[]',
    rank_data_json NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    start_time BIGINT NULL,
    end_time BIGINT NULL,
    window_size INT NOT NULL DEFAULT 0,
    sentiment NVARCHAR(32) NOT NULL DEFAULT '',
    news_count INT NOT NULL DEFAULT 0,
    total_weight FLOAT NOT NULL DEFAULT 0.0,
    heat_change_percent FLOAT NOT NULL DEFAULT 0.0,
    stage NVARCHAR(32) NOT NULL DEFAULT '',
    updated_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    version INT NOT NULL DEFAULT 0,
    CONSTRAINT PK_Topic PRIMARY KEY (created_at, id)
);

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'topic_metrics_history')
CREATE TABLE topic_metrics_history (
    created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    id BIGINT NOT NULL,
    updated_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    topic NVARCHAR(300) NOT NULL,
    llm_title NVARCHAR(300) NULL,
    topic_type NVARCHAR(100) NULL,
    start_time BIGINT NULL,
    end_time BIGINT NULL,
    window_size INT NOT NULL DEFAULT 0,
    sentiment NVARCHAR(32) NOT NULL DEFAULT '',
    news_count INT NOT NULL DEFAULT 0,
    total_weight FLOAT NOT NULL DEFAULT 0.0,
    heat_change_percent FLOAT NOT NULL DEFAULT 0.0,
    stage NVARCHAR(32) NOT NULL DEFAULT '', 
    version INT NOT NULL DEFAULT 0,
    CONSTRAINT PK_topic_metrics_history PRIMARY KEY (created_at, id, updated_at)
);

-- Leftmost-prefix optimized indexes
-- Query pattern A: WHERE topic=? ORDER BY updated_at DESC
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Topic_topic_updated' AND object_id = OBJECT_ID('Topic'))
    CREATE INDEX IX_Topic_topic_updated
    ON Topic(topic, updated_at DESC, created_at DESC, id DESC)
    INCLUDE (total_weight, heat_change_percent, stage, news_count, sentiment);

-- Query pattern B: global recent snapshots scan
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Topic_updated' AND object_id = OBJECT_ID('Topic'))
    CREATE INDEX IX_Topic_updated
    ON Topic(updated_at DESC, created_at DESC, id DESC)
    INCLUDE (topic, total_weight, stage);

-- Query pattern C: history by topic and time
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TMH_topic_snapshot' AND object_id = OBJECT_ID('topic_metrics_history'))
    CREATE INDEX IX_TMH_topic_snapshot
    ON topic_metrics_history(topic, updated_at DESC, created_at, id)
    INCLUDE (total_weight, heat_change_percent, stage, news_count, sentiment);

-- Query pattern D: history by composite key (created_at, id)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TMH_snapshot_time' AND object_id = OBJECT_ID('topic_metrics_history'))
    CREATE INDEX IX_TMH_snapshot_time
    ON topic_metrics_history(created_at, id, updated_at DESC)
    INCLUDE (topic, total_weight, heat_change_percent, stage, news_count, sentiment);

-- Optional partitioning notes:
-- For very large history tables, partition by snapshot_time (monthly/weekly).
-- Typical setup:
-- 1. 創建分區函數 (預留過去、現在、和未來的分界點)
-- 假設我們現在是 2026-03，我們預留從 1 月到 5 月的分區
CREATE PARTITION FUNCTION pf_topic_history_time (BIGINT)
AS RANGE RIGHT FOR VALUES (
    DATEDIFF_BIG(SECOND, '1970-01-01', '2026-03-01 00:00:00'), -- 當前月份
    DATEDIFF_BIG(SECOND, '1970-01-01', '2026-04-01 00:00:00'), -- 預留未來
    DATEDIFF_BIG(SECOND, '1970-01-01', '2026-05-01 00:00:00'),
    DATEDIFF_BIG(SECOND, '1970-01-01', '2026-06-01 00:00:00'), -- 預留未來
    DATEDIFF_BIG(SECOND, '1970-01-01', '2026-07-01 00:00:00')
);

-- 2. 創建分區方案
-- ALL TO ([PRIMARY]) 表示所有分區都放在主文件組，初期開發這樣最簡單
CREATE PARTITION SCHEME ps_topic_history_time
AS PARTITION pf_topic_history_time
ALL TO ([PRIMARY]);
-- 3) Rebuild clustered index / PK of topic_metrics_history onto ps_topic_history_time(snapshot_time).
-- Keep the nonclustered indexes aligned to avoid partition elimination loss.

ALTER DATABASE sentiment_analyze SET READ_COMMITTED_SNAPSHOT ON;