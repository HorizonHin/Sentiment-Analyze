from __future__ import annotations

from datetime import datetime
from typing import List, Optional

try:
    import pyodbc
except ImportError:
    raise ImportError("pyodbc is required for MSSQL backend. Install with: pip install pyodbc")

from SentimentAnalyzeServer.domain.topic.topic import Topic, TopicRepository
from SentimentAnalyzeServer.system.datetime_utils import datetime_to_timestamp, parse_datetime_value


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
    def _dt_to_ts(value: Optional[datetime]) -> int:
        ts = datetime_to_timestamp(value)
        if ts is not None:
            return int(ts)
        now_ts = datetime_to_timestamp(datetime.utcnow())
        return int(now_ts or 0)

    @staticmethod
    def _to_optional_ts(value: Optional[datetime]) -> Optional[int]:
        ts = datetime_to_timestamp(value)
        return int(ts) if ts is not None else None

    @staticmethod
    def _value_to_dt(value) -> Optional[datetime]:
        return parse_datetime_value(value)

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
    def _from_topic_row(row: pyodbc.Row) -> Topic:
        return Topic(
            created_at=SqlServerTopicRepository._value_to_dt(row.created_at),
            id=int(row.id),
            topic=str(row.topic or ""),
            start_time=SqlServerTopicRepository._value_to_dt(row.start_time),
            end_time=SqlServerTopicRepository._value_to_dt(row.end_time),
            window_size=int(row.window_size or 0),
            sentiment=str(row.sentiment or ""),
            news_count=int(row.news_count or 0),
            total_weight=float(row.total_weight or 0.0),
            heat_change_percent=float(row.heat_change_percent or 0.0),
            stage=str(row.stage or ""),
            updated_at=SqlServerTopicRepository._value_to_dt(row.updated_at),
            version=int(row.version or 0),
        )

    def save_topic_snapshot(self, topic: Topic) -> Topic:
        now = datetime.utcnow()
        created_at = topic.created_at or now
        updated_at = topic.updated_at or now
        created_at_ts = self._dt_to_ts(created_at)
        updated_at_ts = self._dt_to_ts(updated_at)

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO Topic (
                    created_at, topic, start_time, end_time, window_size,
                    sentiment, news_count, total_weight, heat_change_percent, stage,
                    updated_at, version
                )
                OUTPUT INSERTED.id
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    def append_topic_metrics_history(self, topic: Topic, snapshot_time: Optional[datetime] = None) -> None:
        if topic.created_at is None or int(topic.id or -1) <= 0:
            return

        write_time = snapshot_time or topic.updated_at or datetime.utcnow()
        created_at_ts = self._dt_to_ts(topic.created_at)
        write_time_ts = self._dt_to_ts(write_time)

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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            )
            conn.commit()
        finally:
            conn.close()

    def get_topic_by_composite_key(self, topic_created_at: datetime, topic_id: int) -> Optional[Topic]:
        created_at_ts = self._dt_to_ts(topic_created_at)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
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
        topic_created_at: datetime,
        topic_id: int,
        limit: int = 100,
    ) -> List[Topic]:
        created_at_ts = self._dt_to_ts(topic_created_at)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP (?)
                    created_at,
                    id,
                    topic,
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

    def get_latest_topic_snapshot(self, topic_name: str) -> Optional[Topic]:
        topic_name = str(topic_name or "").strip()
        if not topic_name:
            return None

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP 1
                    created_at, id, topic, start_time, end_time, window_size,
                    sentiment, news_count, total_weight, heat_change_percent,
                    stage, updated_at, version
                FROM Topic
                WHERE topic = ?
                ORDER BY updated_at DESC, created_at DESC, id DESC
                """,
                topic_name,
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self._from_topic_row(row)
        finally:
            conn.close()

    def list_topic_history(
        self,
        topic_name: str,
        limit: int = 30,
        end_time: Optional[datetime] = None,
    ) -> List[Topic]:
        topic_name = str(topic_name or "").strip()
        if not topic_name:
            return []

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            capped_limit = max(1, int(limit))
            if end_time is not None:
                end_time_ts = self._dt_to_ts(end_time)
                cursor.execute(
                    """
                    SELECT TOP (?)
                        created_at, id, topic, start_time, end_time, window_size,
                        sentiment, news_count, total_weight, heat_change_percent,
                        stage, updated_at, version
                    FROM Topic
                    WHERE topic = ? AND updated_at < ?
                    ORDER BY updated_at DESC, created_at DESC, id DESC
                    """,
                    capped_limit,
                    topic_name,
                    end_time_ts,
                )
            else:
                cursor.execute(
                    """
                    SELECT TOP (?)
                        created_at, id, topic, start_time, end_time, window_size,
                        sentiment, news_count, total_weight, heat_change_percent,
                        stage, updated_at, version
                    FROM Topic
                    WHERE topic = ?
                    ORDER BY updated_at DESC, created_at DESC, id DESC
                    """,
                    capped_limit,
                    topic_name,
                )

            rows = cursor.fetchall()
            return [self._from_topic_row(row) for row in rows]
        finally:
            conn.close()