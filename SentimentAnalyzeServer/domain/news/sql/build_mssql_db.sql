-- MSSQL 数据库初始化脚本
-- 创建新闻数据存储表结构及索引
use sentiment_analyze;

-- 按秒级时间戳分区（BIGINT）
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_news_time')
EXEC ('
CREATE PARTITION FUNCTION pf_news_time (BIGINT)
AS RANGE RIGHT FOR VALUES (
    DATEDIFF_BIG(SECOND, ''1970-01-01'', ''2026-01-01 00:00:00''),
    DATEDIFF_BIG(SECOND, ''1970-01-01'', ''2026-02-01 00:00:00''),
    DATEDIFF_BIG(SECOND, ''1970-01-01'', ''2026-03-01 00:00:00''),
    DATEDIFF_BIG(SECOND, ''1970-01-01'', ''2026-04-01 00:00:00''),
    DATEDIFF_BIG(SECOND, ''1970-01-01'', ''2026-05-01 00:00:00''),
    DATEDIFF_BIG(SECOND, ''1970-01-01'', ''2026-06-01 00:00:00''),
    DATEDIFF_BIG(SECOND, ''1970-01-01'', ''2026-07-01 00:00:00'')
)
');

IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_news_time')
EXEC ('
CREATE PARTITION SCHEME ps_news_time
AS PARTITION pf_news_time
ALL TO ([PRIMARY])
');

-- 创建 NewsItem 表
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'NewsItem')
CREATE TABLE NewsItem (
    id INT IDENTITY(1,1) NOT NULL,
    news_date BIGINT NOT NULL,
    title NVARCHAR(500) NOT NULL,
    source_id NVARCHAR(100) NOT NULL,
    source_name NVARCHAR(100) DEFAULT '',
    event_type NVARCHAR(100) DEFAULT '',
    summary NVARCHAR(2000) DEFAULT '',
    latest_rank INT DEFAULT 0,
    url NVARCHAR(1000) DEFAULT '',
    mobile_url NVARCHAR(1000) DEFAULT '',
    sentiment_polarity NVARCHAR(50) DEFAULT '',
    positive_ratio FLOAT DEFAULT 0.0,
    negative_ratio FLOAT DEFAULT 0.0,
    neutral_ratio FLOAT DEFAULT 0.0,
    optimism_score FLOAT DEFAULT 0.0,
    trust_score FLOAT DEFAULT 0.0,
    controversy_score FLOAT DEFAULT 0.0,
    attention_score FLOAT DEFAULT 0.0,
    first_time BIGINT DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    last_time BIGINT DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    analyzed_time datetime2(3),
    total_weigh FLOAT DEFAULT 0.0,
    CONSTRAINT PK_NewsItem PRIMARY KEY CLUSTERED (first_time, id)
) ON ps_news_time(first_time);

-- 创建 Keyword 表
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Keyword')
CREATE TABLE Keyword (
    id INT IDENTITY(1,1) NOT NULL,
    news_item_id INT NOT NULL,
    news_first_time BIGINT NOT NULL,
    last_time BIGINT DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    term NVARCHAR(500) NOT NULL,
    importance FLOAT DEFAULT 0.0,
    weigh FLOAT DEFAULT 0.0,
    CONSTRAINT PK_Keyword PRIMARY KEY CLUSTERED (news_first_time, news_item_id, id),
    CONSTRAINT FK_Keyword_NewsItem FOREIGN KEY (news_first_time, news_item_id)
        REFERENCES NewsItem(first_time, id) ON DELETE CASCADE,
    UNIQUE(news_first_time, news_item_id, term)
) ON ps_news_time(news_first_time);

-- 创建 Entity 表
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Entity')
CREATE TABLE Entity (
    id INT IDENTITY(1,1) NOT NULL,
    news_item_id INT NOT NULL,
    news_first_time BIGINT NOT NULL,
    last_time BIGINT DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
    name NVARCHAR(200) NOT NULL,
    entity_type NVARCHAR(100) NOT NULL,
    weigh FLOAT DEFAULT 0.0,
    CONSTRAINT PK_Entity PRIMARY KEY CLUSTERED (news_first_time, news_item_id, id),
    CONSTRAINT FK_Entity_NewsItem FOREIGN KEY (news_first_time, news_item_id)
        REFERENCES NewsItem(first_time, id) ON DELETE CASCADE,
    UNIQUE(news_first_time, news_item_id, name, entity_type)
) ON ps_news_time(news_first_time);

-- 创建 rank_timeline 表
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rank_timeline')
CREATE TABLE rank_timeline (
    id INT IDENTITY(1,1) NOT NULL,
    news_item_id INT NOT NULL,
    news_first_time BIGINT NOT NULL,
    timeline_time BIGINT NOT NULL,
    rank_value INT,
    CONSTRAINT PK_rank_timeline PRIMARY KEY CLUSTERED (news_first_time, news_item_id, id),
    CONSTRAINT FK_rank_timeline_NewsItem FOREIGN KEY (news_first_time, news_item_id)
        REFERENCES NewsItem(first_time, id) ON DELETE CASCADE
) ON ps_news_time(news_first_time);

-- 创建索引
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_news_date_last' AND object_id = OBJECT_ID('NewsItem'))
    CREATE INDEX idx_news_date_last ON NewsItem(source_id, title);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_news_source_title' AND object_id = OBJECT_ID('NewsItem'))
    CREATE UNIQUE INDEX ux_news_source_title ON NewsItem(source_id, title) ON [PRIMARY];

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_news_source' AND object_id = OBJECT_ID('NewsItem'))
    CREATE INDEX idx_news_source ON NewsItem(source_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_keyword_news' AND object_id = OBJECT_ID('Keyword'))
    CREATE INDEX idx_keyword_news ON Keyword(news_first_time, news_item_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_keyword_term' AND object_id = OBJECT_ID('Keyword'))
    CREATE INDEX idx_keyword_term ON Keyword(term, news_first_time, news_item_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_keyword_last_time' AND object_id = OBJECT_ID('Keyword'))
    CREATE INDEX idx_keyword_last_time ON Keyword(news_first_time, last_time, news_item_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_entity_news' AND object_id = OBJECT_ID('Entity'))
    CREATE INDEX idx_entity_news ON Entity(news_first_time, news_item_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_entity_name' AND object_id = OBJECT_ID('Entity'))
    CREATE INDEX idx_entity_name ON Entity(name, news_first_time, news_item_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_entity_last_time' AND object_id = OBJECT_ID('Entity'))
    CREATE INDEX idx_entity_last_time ON Entity(news_first_time, last_time, news_item_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_timeline_news' AND object_id = OBJECT_ID('rank_timeline'))
    CREATE INDEX idx_timeline_news ON rank_timeline(news_first_time, news_item_id, timeline_time);
