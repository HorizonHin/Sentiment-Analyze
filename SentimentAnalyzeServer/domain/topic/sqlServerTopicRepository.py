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

    def _iter_default_window_starts(self, base_end_ts: int) -> List[Optional[int]]:
        starts: List[Optional[int]] = []
        for step in range(1, 4):
            starts.append(int(base_end_ts) - self.first_time_lookback_seconds * step)
        starts.append(None)
        return starts

    def add_topics(self, topics: List[Topic]) -> List[Topic]:
        if not topics:
            return []
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            now = int(time.time())
            results = []
            for topic in topics:
                created_at = topic.created_at or now
                updated_at = topic.updated_at or now
                created_at_ts = self._to_ts(created_at)
                updated_at_ts = self._to_ts(updated_at)
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
                topic.id = inserted_id
                topic.created_at = created_at
                topic.updated_at = updated_at
                results.append(topic)
            conn.commit()
            return results
        finally:
            conn.close()

    def append_topic_metrics_histories(self, topics: List[Topic], snapshot_time: Optional[int] = None) -> None:
        if not topics:
            return
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            now = int(time.time())
            for topic in topics:
                if topic.created_at is None or int(topic.id or -1) <= 0:
                    continue
                write_time = snapshot_time or topic.updated_at or now
                created_at_ts = self._to_ts(topic.created_at)
                write_time_ts = self._to_ts(write_time)
                cursor.execute(
                    """
                    INSERT INTO topic_metrics_history (
                        created_at, topic, llm_title, start_time, end_time, window_size,
                        sentiment, news_count, total_weight, heat_change_percent,
                        stage, updated_at, version, id
                    )
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM topic_metrics_history
                        WHERE created_at = ? AND id = ? AND updated_at = ?
                    )
                    """,
                    created_at_ts,
                    topic.topic,
                    (None if topic.llm_title is None else str(topic.llm_title)),
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

    def update_topics(self, topics: List[Topic]) -> List[Topic]:
        if not topics:
            return []
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            now = int(time.time())
            results = []
            for topic in topics:
                if topic.created_at is None or topic.id is None:
                    continue
                created_at_ts = self._to_ts(topic.created_at)
                updated_at_ts = self._to_ts(now)
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
                    int(topic.id),
                )
                old_row = cursor.fetchone()
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
                    int(topic.id),
                )
                topic.updated_at = updated_at_ts
                topic.version = (int(old_row.version or 0) + 1) if old_row else (int(topic.version or 0) + 1)
                results.append(topic)
            conn.commit()
            return results
        finally:
            conn.close()

    def list_topics_by_time_range(
            self,
            created_at_start: Optional[int] = None,
            created_at_end: Optional[int] = None,
            updated_at_start: Optional[int] = None,
            updated_at_end: Optional[int] = None,
            limit: int = 100,
        ) -> List[Topic]:
            """
            根据created_at和updated_at的起止时间，返回Topic表中的所有Topic。
            """
            use_default_window = created_at_start is None and updated_at_start is None
            base_end_ts = int(updated_at_end or created_at_end or time.time())

            if use_default_window:
                created_start_candidates = self._iter_default_window_starts(base_end_ts)
                updated_start_candidates = self._iter_default_window_starts(base_end_ts)
            else:
                created_start_candidates = [created_at_start]
                updated_start_candidates = [updated_at_start]

            for idx in range(len(created_start_candidates)):
                effective_created_start = created_start_candidates[idx]
                effective_updated_start = updated_start_candidates[idx]

                conditions = []
                params = []
                if effective_created_start is not None:
                    conditions.append("created_at >= ?")
                    params.append(int(effective_created_start))
                if created_at_end is not None:
                    conditions.append("created_at <= ?")
                    params.append(int(created_at_end))
                if effective_updated_start is not None:
                    conditions.append("updated_at >= ?")
                    params.append(int(effective_updated_start))
                if updated_at_end is not None:
                    conditions.append("updated_at <= ?")
                    params.append(int(updated_at_end))

                where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
                sql = f"""
                    SELECT TOP (?)
                        created_at, id, topic, llm_title, topic_type, platform_distribution_json, rank_data_json, start_time, end_time, window_size,
                        sentiment, news_count, total_weight, heat_change_percent,
                        stage, updated_at, version
                    FROM Topic
                    {where_clause}
                    ORDER BY  total_weight DESC,updated_at DESC
                """
                conn = self._get_connection()
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, int(limit), *params)
                    rows = cursor.fetchall()
                    if rows:
                        return [self._from_topic_row(row) for row in rows]
                finally:
                    conn.close()

            return []
    
    def __init__(
        self,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: str = "ODBC Driver 17 for SQL Server",
        first_time_lookback_days: int = 7,
    ) -> None:
        self.server = server or "localhost"
        self.database = database or "sentiment_analyze"
        self.username = username or "sa"
        self.password = password or ""
        self.driver = driver
        self.first_time_lookback_seconds = max(1, int(first_time_lookback_days)) * 24 * 60 * 60

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
        
        # 启用 pyodbc 连接池
        pyodbc.pooling = True

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

    def find_topic_by_name(
        self,
        topic_name: str,
    ) -> Optional[Topic]:
        """
        按名称查找 Topic，默认使用 first_time_lookback_days 的窗口回溯策略。
        
        回溯顺序：1x、2x、3x lookback window，最后全量兜底。
        """
        topic_name = str(topic_name or "").strip()
        if not topic_name:
            return None

        now_ts = int(time.time())
        for start_ts in self._iter_default_window_starts(now_ts):
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                if start_ts is None:
                    cursor.execute(
                        """
                        SELECT TOP 1
                            created_at, id, topic, llm_title, topic_type, platform_distribution_json, rank_data_json, start_time, end_time, window_size,
                            sentiment, news_count, total_weight, heat_change_percent,
                            stage, updated_at, version
                        FROM Topic
                        WHERE topic = ?
                        ORDER BY updated_at DESC, created_at DESC, id DESC
                        """,
                        topic_name,
                    )
                else:
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
                        start_ts,
                        start_ts,
                    )
                row = cursor.fetchone()
                if row is not None:
                    return self._from_topic_row(row)
            finally:
                conn.close()

        return None

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