use sentiment_analyze
IF OBJECT_ID('NewsItem', 'U') IS NULL
BEGIN
    CREATE TABLE NewsItem (
        id INT PRIMARY KEY IDENTITY(1,1),
        news_date DATE NOT NULL,                      -- 改为 DATE (YYYY-MM-DD)
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
        first_time DATETIME2 DEFAULT GETDATE(),      -- 改为 DATETIME2
        last_time DATETIME2 DEFAULT GETDATE(),       -- 改为 DATETIME2
        analyzed_time DATETIME2,                     -- 改为 DATETIME2
        total_weigh FLOAT DEFAULT 0.0,
        CONSTRAINT UQ_NewsItem_Source_Title UNIQUE(source_id, title)
    );
END

-- 2. 创建 Keyword 表
IF OBJECT_ID('Keyword', 'U') IS NULL
BEGIN
    CREATE TABLE Keyword (
        id INT PRIMARY KEY IDENTITY(1,1),
        news_item_id INT NOT NULL,
        term NVARCHAR(500) NOT NULL,
        create_time DATETIME2 DEFAULT GETDATE(),     -- 改为 DATETIME2
        importance FLOAT DEFAULT 0.0,
        FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
    );
END

-- 3. 创建 Entity 表
IF OBJECT_ID('Entity', 'U') IS NULL
BEGIN
    CREATE TABLE Entity (
        id INT PRIMARY KEY IDENTITY(1,1),
        news_item_id INT NOT NULL,
        name NVARCHAR(200) NOT NULL,
        entity_type NVARCHAR(100) NOT NULL,
        create_time DATETIME2 DEFAULT GETDATE(),     -- 改为 DATETIME2
        FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
    );
END

-- 4. 创建 rank_timeline 表
IF OBJECT_ID('rank_timeline', 'U') IS NULL
BEGIN
    CREATE TABLE rank_timeline (
        id INT PRIMARY KEY IDENTITY(1,1),
        news_item_id INT NOT NULL,
        timeline_time DATETIME2 NOT NULL,            -- 改为 DATETIME2
        rank_value INT,
        FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
    );
END

-- 5. 索引保持不变
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

ALTER DATABASE sentiment_analyze COLLATE Chinese_PRC_CI_AS;