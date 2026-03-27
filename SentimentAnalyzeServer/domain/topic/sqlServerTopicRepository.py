from __future__ import annotations

import json
import time
from typing import List, Optional

try:
    import pyodbc
except ImportError:
    raise ImportError("pyodbc is required for MSSQL backend. Install with: pip install pyodbc")

from SentimentAnalyzeServer.domain.news.news import NewsItem
from SentimentAnalyzeServer.domain.topic.topic import Topic, TopicPlatformStats, TopicRepository


class SqlServerTopicRepository(TopicRepository):
    def __init__(
        self,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: str = "ODBC Driver 17 for SQL Server",
    ) -> None:
        self.server = server or "localhost"
        self.database = database or "sentiment_analyze"
        self.username = username or "sa"
        self.password = password or ""
        self.driver = driver

        if self.password:
            self.connection_string = (
                f"Driver={{{self.driver}}};"
                f"Server={self.server};"
                f"Database={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "Trusted_Connection=no;"
            )
        else:
            self.connection_string = (
                f"Driver={{{self.driver}}};"
                f"Server={self.server};"
                f"Database={self.database};"
                "Trusted_Connection=yes;"
            )

        self._ensure_schema()

    def _get_connection(self) -> pyodbc.Connection:
        return pyodbc.connect(self.connection_string, timeout=10)

    @staticmethod
    def _to_ts(value: Optional[object]) -> int:
        if value is None:
            return int(time.time())
        if isinstance(value, int):
            return int(value)
        raise TypeError(f"timestamp must be int, got {type(value).__name__}")

    @staticmethod
    def _to_optional_ts(value: Optional[object]) -> Optional[int]:
        if value is None:
            return None
        return SqlServerTopicRepository._to_ts(value)

    @staticmethod
    def _value_to_ts(value) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, int):
            return int(value)
        raise TypeError(f"timestamp must be int, got {type(value).__name__}")

    def _ensure_schema(self) -> None:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Topic')
                CREATE TABLE Topic (
                    created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
                    id BIGINT IDENTITY(1,1) NOT NULL,
                    topic NVARCHAR(300) NOT NULL,
                    llm_title NVARCHAR(300) NULL,
                    topic_type NVARCHAR(64) NULL,
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
                )
                """
            )

            cursor.execute(
                """
                IF COL_LENGTH('Topic', 'platform_distribution_json') IS NULL
                    ALTER TABLE Topic ADD platform_distribution_json NVARCHAR(MAX) NOT NULL DEFAULT '[]'
                """
            )
            cursor.execute(
                """
                IF COL_LENGTH('Topic', 'rank_data_json') IS NULL
                    ALTER TABLE Topic ADD rank_data_json NVARCHAR(MAX) NOT NULL DEFAULT '{}'
                """
            )
            cursor.execute(
                """
                IF COL_LENGTH('Topic', 'llm_title') IS NULL
                    ALTER TABLE Topic ADD llm_title NVARCHAR(300) NULL
                """
            )
            cursor.execute(
                """
                IF COL_LENGTH('Topic', 'topic_type') IS NULL
                    ALTER TABLE Topic ADD topic_type NVARCHAR(64) NULL
                """
            )

            cursor.execute(
                """
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'topic_metrics_history')
                CREATE TABLE topic_metrics_history (
                    created_at BIGINT NOT NULL DEFAULT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME()),
                    id BIGINT NOT NULL,
                    topic NVARCHAR(300) NOT NULL,
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
                    CONSTRAINT PK_topic_metrics_history PRIMARY KEY (created_at, id, updated_at)
                )
                """
            )

            cursor.execute(
                """
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Topic_topic_updated' AND object_id = OBJECT_ID('Topic'))
                    CREATE INDEX IX_Topic_topic_updated
                    ON Topic(topic, updated_at DESC, created_at DESC, id DESC)
                    INCLUDE (total_weight, heat_change_percent, stage, news_count, sentiment)
                """
            )

            cursor.execute(
                """
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Topic_updated' AND object_id = OBJECT_ID('Topic'))
                    CREATE INDEX IX_Topic_updated
                    ON Topic(updated_at DESC, created_at DESC, id DESC)
                    INCLUDE (topic, total_weight, stage)
                """
            )

            cursor.execute(
                """
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TMH_topic_snapshot' AND object_id = OBJECT_ID('topic_metrics_history'))
                    CREATE INDEX IX_TMH_topic_snapshot
                    ON topic_metrics_history(topic, updated_at DESC, created_at, id)
                    INCLUDE (total_weight, heat_change_percent, stage, news_count, sentiment)
                """
            )

            cursor.execute(
                """
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TMH_snapshot_time' AND object_id = OBJECT_ID('topic_metrics_history'))
                    CREATE INDEX IX_TMH_snapshot_time
                    ON topic_metrics_history(created_at, id, updated_at DESC)
                    INCLUDE (topic, total_weight, heat_change_percent, stage, news_count, sentiment)
                """
            )

            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _safe_json_load(text: Optional[str], default):
        if not text:
            return default
        try:
            return json.loads(text)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _serialize_platform_distribution(topic: Topic) -> str:
        payload = [item.to_dict() for item in (topic.platform_distribution or [])]
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _serialize_rank_data(topic: Topic) -> str:
        payload = {
            key: [item.to_dict() for item in items]
            for key, items in (topic.rank_data or {}).items()
        }
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _row_get(row: pyodbc.Row, name: str, default=None):
        try:
            return getattr(row, name)
        except AttributeError:
            return default

    @staticmethod
    def _from_topic_row(row: pyodbc.Row) -> Topic:
        raw_platform_distribution = SqlServerTopicRepository._safe_json_load(
            SqlServerTopicRepository._row_get(row, "platform_distribution_json", "[]"),
            [],
        )
        platform_distribution = [
            TopicPlatformStats.from_dict(item)
            for item in raw_platform_distribution
            if isinstance(item, dict)
        ]

        raw_rank_data = SqlServerTopicRepository._safe_json_load(
            SqlServerTopicRepository._row_get(row, "rank_data_json", "{}"),
            {},
        )
        rank_data = {
            str(key): [NewsItem.from_dict(item) for item in items if isinstance(item, dict)]
            for key, items in raw_rank_data.items()
            if isinstance(items, list)
        }

        return Topic(
            created_at=SqlServerTopicRepository._value_to_ts(row.created_at),
            id=int(row.id),
            topic=str(row.topic or ""),
            llm_title=(None if SqlServerTopicRepository._row_get(row, "llm_title") is None else str(SqlServerTopicRepository._row_get(row, "llm_title") or "")),
            topic_type=(None if SqlServerTopicRepository._row_get(row, "topic_type") is None else str(SqlServerTopicRepository._row_get(row, "topic_type") or "")),
            platform_distribution=platform_distribution,
            rank_data=rank_data,
            start_time=SqlServerTopicRepository._value_to_ts(row.start_time),
            end_time=SqlServerTopicRepository._value_to_ts(row.end_time),
            window_size=int(row.window_size or 0),
            sentiment=str(row.sentiment or ""),
            news_count=int(row.news_count or 0),
            total_weight=float(row.total_weight or 0.0),
            heat_change_percent=float(row.heat_change_percent or 0.0),
            stage=str(row.stage or ""),
            updated_at=SqlServerTopicRepository._value_to_ts(row.updated_at),
            version=int(row.version or 0),
        )

    def add_topic(self, topic: Topic) -> Topic:
        now = int(time.time())
        created_at = topic.created_at or now
        updated_at = topic.updated_at or now
        created_at_ts = self._to_ts(created_at)
        updated_at_ts = self._to_ts(updated_at)

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # 插入Topic主表
            cursor.execute(
                """
                INSERT INTO Topic (
                    created_at, topic, llm_title, topic_type, platform_distribution_json, rank_data_json, start_time, end_time, window_size,
                    sentiment, news_count, total_weight, heat_change_percent, stage,
                    updated_at, version
                )
                OUTPUT INSERTED.id
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                created_at_ts,
                topic.topic,
                (None if topic.llm_title is None else str(topic.llm_title)),
                (None if topic.topic_type is None else str(topic.topic_type)),
                self._serialize_platform_distribution(topic),
                self._serialize_rank_data(topic),
                self._to_optional_ts(topic.start_time),
                self._to_optional_ts(topic.end_time),
                int(topic.window_size or 0),
                topic.sentiment,
                int(topic.news_count or 0),
                float(topic.total_weight or 0.0),
                float(topic.heat_change_percent or 0.0),
                topic.stage,
                updated_at_ts,
                int(topic.version or 0),
            )
            inserted = cursor.fetchone()
            inserted_id = int(inserted[0]) if inserted and inserted[0] is not None else -1

            conn.commit()
            topic.id = inserted_id
            topic.created_at = created_at
            topic.updated_at = updated_at
            return topic
        finally:
            conn.close()

    def append_topic_metrics_history(self, topic: Topic, snapshot_time: Optional[int] = None) -> None:
        if topic.created_at is None or int(topic.id or -1) <= 0:
            return

        write_time = snapshot_time or topic.updated_at or int(time.time())
        created_at_ts = self._to_ts(topic.created_at)
        write_time_ts = self._to_ts(write_time)

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO topic_metrics_history (
                    created_at, topic, start_time, end_time, window_size,
                    sentiment, news_count, total_weight, heat_change_percent,
                    stage, updated_at, version, id
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM topic_metrics_history
                    WHERE created_at = ? AND id = ? AND updated_at = ?
                )
                """,
                created_at_ts,
                topic.topic,
                self._to_optional_ts(topic.start_time),
                self._to_optional_ts(topic.end_time),
                int(topic.window_size or 0),
                topic.sentiment,
                int(topic.news_count or 0),
                float(topic.total_weight or 0.0),
                float(topic.heat_change_percent or 0.0),
                topic.stage,
                write_time_ts,
                int(topic.version or 0),
                int(topic.id),
                created_at_ts,
                int(topic.id),
                write_time_ts,
            )
            conn.commit()
        finally:
            conn.close()

    def get_topic_by_composite_key(self, topic_created_at: int, topic_id: int) -> Optional[Topic]:
        created_at_ts = self._to_ts(topic_created_at)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP 1
                    created_at, id, topic, llm_title, topic_type, platform_distribution_json, rank_data_json, start_time, end_time, window_size,
                    sentiment, news_count, total_weight, heat_change_percent,
                    stage, updated_at, version
                FROM Topic
                WHERE created_at = ? AND id = ?
                """,
                created_at_ts,
                int(topic_id),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self._from_topic_row(row)
        finally:
            conn.close()

    def list_topic_metrics_history_by_composite_key(
        self,
        topic_created_at: int,
        topic_id: int,
        limit: int = 100,
    ) -> List[Topic]:
        created_at_ts = self._to_ts(topic_created_at)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP (?)
                    created_at,
                    id,
                    topic,
                    CAST(NULL AS NVARCHAR(MAX)) AS platform_distribution_json,
                    CAST(NULL AS NVARCHAR(MAX)) AS rank_data_json,
                    start_time,
                    end_time,
                    window_size,
                    sentiment,
                    news_count,
                    total_weight,
                    heat_change_percent,
                    stage,
                    updated_at,
                    version
                FROM topic_metrics_history
                WHERE created_at = ? AND id = ?
                ORDER BY updated_at DESC
                """,
                max(1, int(limit)),
                created_at_ts,
                int(topic_id),
            )

            rows = cursor.fetchall()
            return [self._from_topic_row(row) for row in rows]
        finally:
            conn.close()

    def find_recent_topic_by_name(
        self,
        topic_name: str,
        days_lookback: int = 7,
    ) -> Optional[Topic]:
        """
        查找相同topic名称、且updated_at在最近N天内的最新Topic记录。
        
        如果存在这样的记录，返回其信息（包括created_at和id），
        以便后续update操作能够使用正确的主键。
        
        Args:
            topic_name: Topic名称
            days_lookback: 向后查看的天数（默认7天）
        
        Returns:
            Topic对象（如果存在）或None
        """
        topic_name = str(topic_name or "").strip()
        if not topic_name:
            return None

        now = int(time.time())
        lookback_seconds = days_lookback * 86400
        cutoff_time_ts = now - lookback_seconds

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP 1
                    created_at, id, topic, llm_title, topic_type, platform_distribution_json, rank_data_json, start_time, end_time, window_size,
                    sentiment, news_count, total_weight, heat_change_percent,
                    stage, updated_at, version
                FROM Topic
                WHERE topic = ? AND created_at >= ? AND updated_at >= ?
                ORDER BY updated_at DESC, created_at DESC, id DESC
                """,
                topic_name,
                cutoff_time_ts,
                cutoff_time_ts,
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self._from_topic_row(row)
        finally:
            conn.close()

    def update_topic(
        self,
        topic: Topic,
        existing_created_at: int,
        existing_id: int,
    ) -> Topic:
        """
        更新现有Topic，保持主键(created_at, id)不变。
        不更新llm_title字段，保持原有值不变，由单独的接口 update_topic_llm_title_only 来更新。
        
        Args:
            topic: 新的Topic数据
            existing_created_at: 要更新的Topic的created_at（主键）
            existing_id: 要更新的Topic的id（主键）
        
        Returns:
            更新后的Topic对象
        """
        now = int(time.time())
        created_at_ts = self._to_ts(existing_created_at)
        updated_at_ts = self._to_ts(now)

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # 先获取旧数据用于历史记录
            cursor.execute(
                """
                SELECT TOP 1
                    created_at, id, topic, start_time, end_time, window_size,
                    sentiment, news_count, total_weight, heat_change_percent,
                    stage, updated_at, version
                FROM Topic
                WHERE created_at = ? AND id = ?
                """,
                created_at_ts,
                int(existing_id),
            )
            old_row = cursor.fetchone()

            # 执行UPDATE操作
            cursor.execute(
                """
                UPDATE Topic
                SET
                    topic_type = ?,
                    platform_distribution_json = ?,
                    rank_data_json = ?,
                    start_time = ?,
                    end_time = ?,
                    window_size = ?,
                    sentiment = ?,
                    news_count = ?,
                    total_weight = ?,
                    heat_change_percent = ?,
                    stage = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE created_at = ? AND id = ?
                """,
                (None if topic.topic_type is None else str(topic.topic_type)),
                self._serialize_platform_distribution(topic),
                self._serialize_rank_data(topic),
                self._to_optional_ts(topic.start_time),
                self._to_optional_ts(topic.end_time),
                int(topic.window_size or 0),
                topic.sentiment,
                int(topic.news_count or 0),
                float(topic.total_weight or 0.0),
                float(topic.heat_change_percent or 0.0),
                topic.stage,
                updated_at_ts,
                created_at_ts,
                int(existing_id),
            )
            conn.commit()
            # 返回更新后的Topic对象
            topic.id = int(existing_id)
            topic.created_at = int(existing_created_at)
            topic.updated_at = updated_at_ts
            topic.version = (int(old_row.version or 0) + 1) if old_row else (int(topic.version or 0) + 1)
            return topic
        finally:
            conn.close()

    def list_topics_missing_llm_title(self, limit: int = 50) -> List[Topic]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP (?)
                    created_at, id, topic, llm_title, topic_type, platform_distribution_json, rank_data_json, start_time, end_time, window_size,
                    sentiment, news_count, total_weight, heat_change_percent,
                    stage, updated_at, version
                FROM Topic
                WHERE LTRIM(RTRIM(COALESCE(llm_title, ''))) = ''
                ORDER BY updated_at DESC, created_at DESC, id DESC
                """,
                max(1, int(limit)),
            )
            rows = cursor.fetchall()
            return [self._from_topic_row(row) for row in rows]
        finally:
            conn.close()

    def update_topic_llm_title_only(
        self,
        topic_created_at: int,
        topic_id: int,
        llm_title: str,
    ) -> bool:
        created_at_ts = self._to_ts(topic_created_at)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE Topic
                SET llm_title = ?
                WHERE created_at = ? AND id = ?
                """,
                str(llm_title or "").strip(),
                created_at_ts,
                int(topic_id),
            )
            conn.commit()
            return int(cursor.rowcount or 0) > 0
        finally:
            conn.close()