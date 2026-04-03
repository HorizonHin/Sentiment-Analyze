-- Risk warning and sensitive title audit tables for Topic-level early warning.

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'topic_risk_warning')
CREATE TABLE topic_risk_warning (
    id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    topic_created_at BIGINT NOT NULL,
    topic_id BIGINT NOT NULL,
    topic_name NVARCHAR(300) NOT NULL DEFAULT '',
    risk_type NVARCHAR(64) NOT NULL,
    risk_level NVARCHAR(16) NOT NULL,
    risk_score INT NOT NULL DEFAULT 0,
    reason NVARCHAR(1000) NOT NULL DEFAULT '',
    metrics_json NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    detected_by_event NVARCHAR(128) NOT NULL DEFAULT '',
    occurred_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    CONSTRAINT FK_topic_risk_warning_topic FOREIGN KEY (topic_created_at, topic_id)
        REFERENCES Topic(created_at, id)
);

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'topic_sensitive_title_audit')
CREATE TABLE topic_sensitive_title_audit (
    id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    topic_created_at BIGINT NOT NULL,
    topic_id BIGINT NOT NULL,
    topic_name NVARCHAR(300) NOT NULL DEFAULT '',
    old_topic NVARCHAR(300) NOT NULL DEFAULT '',
    candidate_titles_json NVARCHAR(MAX) NOT NULL DEFAULT '[]',
    reason NVARCHAR(256) NOT NULL DEFAULT '',
    risk_level NVARCHAR(16) NOT NULL DEFAULT 'high',
    context_json NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    occurred_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    CONSTRAINT FK_topic_sensitive_title_audit_topic FOREIGN KEY (topic_created_at, topic_id)
        REFERENCES Topic(created_at, id)
);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_topic_risk_warning_topic' AND object_id = OBJECT_ID('topic_risk_warning'))
    CREATE INDEX IX_topic_risk_warning_topic
    ON topic_risk_warning(topic_created_at, topic_id, occurred_at DESC);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_topic_risk_warning_type_level' AND object_id = OBJECT_ID('topic_risk_warning'))
    CREATE INDEX IX_topic_risk_warning_type_level
    ON topic_risk_warning(risk_type, risk_level, occurred_at DESC);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_topic_risk_warning_dedup' AND object_id = OBJECT_ID('topic_risk_warning'))
    CREATE UNIQUE INDEX UX_topic_risk_warning_dedup
    ON topic_risk_warning(topic_created_at, topic_id, risk_type, occurred_at);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_topic_sensitive_title_topic' AND object_id = OBJECT_ID('topic_sensitive_title_audit'))
    CREATE INDEX IX_topic_sensitive_title_topic
    ON topic_sensitive_title_audit(topic_created_at, topic_id, occurred_at DESC);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_topic_sensitive_title_dedup' AND object_id = OBJECT_ID('topic_sensitive_title_audit'))
    CREATE UNIQUE INDEX UX_topic_sensitive_title_dedup
    ON topic_sensitive_title_audit(topic_created_at, topic_id, reason, occurred_at);
