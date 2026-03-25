-- MSSQL 数据库初始化脚本
-- 创建新闻数据存储表结构及索引

-- 创建 NewsItem 表
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'NewsItem')
CREATE TABLE NewsItem (
    id INT PRIMARY KEY IDENTITY(1,1),
    news_date DATE NOT NULL,
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
    first_time DATETIME2 DEFAULT GETDATE(),
    last_time DATETIME2 DEFAULT GETDATE(),
    analyzed_time DATETIME2,
    total_weigh FLOAT DEFAULT 0.0,
    UNIQUE(source_id, title)
);

-- 创建 Keyword 表
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Keyword')
CREATE TABLE Keyword (
    id INT PRIMARY KEY IDENTITY(1,1),
    news_item_id INT NOT NULL,
    term NVARCHAR(500) NOT NULL,
    last_time DATETIME2 DEFAULT GETDATE(),
    importance FLOAT DEFAULT 0.0,
    weigh FLOAT DEFAULT 0.0,
    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE,
    UNIQUE(news_item_id, term)
);

-- 创建 Entity 表
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Entity')
CREATE TABLE Entity (
    id INT PRIMARY KEY IDENTITY(1,1),
    news_item_id INT NOT NULL,
    name NVARCHAR(200) NOT NULL,
    entity_type NVARCHAR(100) NOT NULL,
    last_time DATETIME2 DEFAULT GETDATE(),
    weigh FLOAT DEFAULT 0.0,
    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE,
    UNIQUE(news_item_id, name, entity_type)
);

-- 迁移兼容性：重命名列
IF COL_LENGTH('Keyword', 'last_time') IS NULL AND COL_LENGTH('Keyword', 'create_time') IS NOT NULL
    EXEC sp_rename 'Keyword.create_time', 'last_time', 'COLUMN';

IF COL_LENGTH('Entity', 'last_time') IS NULL AND COL_LENGTH('Entity', 'create_time') IS NOT NULL
    EXEC sp_rename 'Entity.create_time', 'last_time', 'COLUMN';

-- 迁移兼容性：添加缺失列
IF COL_LENGTH('Keyword', 'last_time') IS NULL
    ALTER TABLE Keyword ADD last_time DATETIME2 DEFAULT GETDATE();

IF COL_LENGTH('Entity', 'last_time') IS NULL
    ALTER TABLE Entity ADD last_time DATETIME2 DEFAULT GETDATE();

IF COL_LENGTH('Keyword', 'weigh') IS NULL
    ALTER TABLE Keyword ADD weigh FLOAT DEFAULT 0.0;

IF COL_LENGTH('Entity', 'weigh') IS NULL
    ALTER TABLE Entity ADD weigh FLOAT DEFAULT 0.0;

-- 创建 rank_timeline 表
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rank_timeline')
CREATE TABLE rank_timeline (
    id INT PRIMARY KEY IDENTITY(1,1),
    news_item_id INT NOT NULL,
    timeline_time DATETIME2 NOT NULL,
    rank_value INT,
    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
);

-- 创建索引
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_news_date_last' AND object_id = OBJECT_ID('NewsItem'))
    CREATE INDEX idx_news_date_last ON NewsItem(source_id, title);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_news_source' AND object_id = OBJECT_ID('NewsItem'))
    CREATE INDEX idx_news_source ON NewsItem(source_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_keyword_news' AND object_id = OBJECT_ID('Keyword'))
    CREATE INDEX idx_keyword_news ON Keyword(news_item_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_entity_news' AND object_id = OBJECT_ID('Entity'))
    CREATE INDEX idx_entity_news ON Entity(news_item_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_timeline_news' AND object_id = OBJECT_ID('rank_timeline'))
    CREATE INDEX idx_timeline_news ON rank_timeline(news_item_id);
