from __future__ import annotations

import json
import time
from typing import List, Optional

try:
    import pyodbc
except ImportError:
    raise ImportError("pyodbc is required for MSSQL backend. Install with: pip install pyodbc")

from SentimentAnalyzeServer.domain.risk.risk import RiskWarningRepository, SensitiveTitleRecord, TopicRiskWarning


class SqlServerRiskWarningRepository(RiskWarningRepository):
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
        
        # 启用 pyodbc 连接池
        pyodbc.pooling = True

    def _get_connection(self) -> pyodbc.Connection:
        return pyodbc.connect(self.connection_string, timeout=10)

    @staticmethod
    def _to_ts(value: Optional[object]) -> int:
        if value is None:
            return int(time.time())
        if isinstance(value, bool):
            raise TypeError(f"timestamp must be int, got {type(value).__name__}")
        if isinstance(value, int):
            return int(value)
        if isinstance(value, str):
            text = value.strip()
            if text and (text.isdigit() or (text.startswith("-") and text[1:].isdigit())):
                return int(text)
        raise TypeError(f"timestamp must be int, got {type(value).__name__}")

    @staticmethod
    def _to_json(value, default) -> str:
        payload = value if value is not None else default
        return json.dumps(payload, ensure_ascii=False)

    def add_topic_risk_warnings(self, warnings: List[TopicRiskWarning]) -> int:
        if not warnings:
            return 0

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            inserted = 0
            for warning in warnings:
                if int(warning.topic_created_at or 0) <= 0 or int(warning.topic_id or -1) <= 0:
                    continue
                occurred_at = self._to_ts(warning.occurred_at)
                cursor.execute(
                    """
                    INSERT INTO topic_risk_warning (
                        topic_created_at,
                        topic_id,
                        topic_name,
                        risk_type,
                        risk_level,
                        risk_score,
                        reason,
                        metrics_json,
                        detected_by_event,
                        occurred_at,
                        created_at
                    )
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM topic_risk_warning
                        WHERE topic_created_at = ?
                          AND topic_id = ?
                          AND risk_type = ?
                          AND occurred_at = ?
                    )
                    """,
                    int(warning.topic_created_at),
                    int(warning.topic_id),
                    str(warning.topic_name or ""),
                    str(warning.risk_type or ""),
                    str(warning.risk_level or ""),
                    int(warning.risk_score or 0),
                    str(warning.reason or ""),
                    self._to_json(warning.metrics, {}),
                    str(warning.detected_by_event or ""),
                    occurred_at,
                    int(time.time()),
                    int(warning.topic_created_at),
                    int(warning.topic_id),
                    str(warning.risk_type or ""),
                    occurred_at,
                )
                if int(cursor.rowcount or 0) > 0:
                    inserted += 1

            conn.commit()
            return inserted
        finally:
            conn.close()

    def get_topic_risk_warnings(
        self,
        topic_created_at: Optional[int] = None,
        topic_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        risk_level: Optional[str] = None,
        limit: int = 100,
    ) -> List[TopicRiskWarning]:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            where_clauses = []
            params = []

            if topic_created_at is not None:
                where_clauses.append("topic_created_at = ?")
                params.append(int(topic_created_at))
            if topic_id is not None:
                where_clauses.append("topic_id = ?")
                params.append(int(topic_id))
            if risk_level:
                where_clauses.append("risk_level = ?")
                params.append(str(risk_level))
            if start_time is not None:
                where_clauses.append("occurred_at >= ?")
                params.append(int(start_time))
            if end_time is not None:
                where_clauses.append("occurred_at <= ?")
                params.append(int(end_time))

            where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            query = f"""
                SELECT TOP (?)
                    topic_created_at, topic_id, topic_name, risk_type, risk_level,
                    risk_score, reason, metrics_json, detected_by_event, occurred_at
                FROM topic_risk_warning
                {where_sql}
                ORDER BY occurred_at DESC, id DESC
            """
            cursor.execute(query, limit, *params)
            
            results = []
            for row in cursor.fetchall():
                results.append(TopicRiskWarning(
                    topic_created_at=int(row[0]),
                    topic_id=int(row[1]),
                    topic_name=str(row[2]),
                    risk_type=str(row[3]),
                    risk_level=str(row[4]),
                    risk_score=int(row[5]),
                    reason=str(row[6]),
                    metrics=json.loads(row[7]) if row[7] else {},
                    detected_by_event=str(row[8]),
                    occurred_at=int(row[9])
                ))
            return results
        finally:
            conn.close()

    def add_sensitive_title_records(self, records: List[SensitiveTitleRecord]) -> int:
        if not records:
            return 0

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            inserted = 0
            for record in records:
                if int(record.topic_created_at or 0) <= 0 or int(record.topic_id or -1) <= 0:
                    continue
                occurred_at = self._to_ts(record.occurred_at)
                reason = str(record.reason or "")
                cursor.execute(
                    """
                    INSERT INTO topic_sensitive_title_audit (
                        topic_created_at,
                        topic_id,
                        topic_name,
                        old_topic,
                        candidate_titles_json,
                        reason,
                        risk_level,
                        context_json,
                        occurred_at,
                        created_at
                    )
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM topic_sensitive_title_audit
                        WHERE topic_created_at = ?
                          AND topic_id = ?
                          AND reason = ?
                          AND occurred_at = ?
                    )
                    """,
                    int(record.topic_created_at),
                    int(record.topic_id),
                    str(record.topic_name or ""),
                    str(record.old_topic or ""),
                    self._to_json(record.candidate_titles, []),
                    reason,
                    str(record.risk_level or ""),
                    self._to_json(record.context, {}),
                    occurred_at,
                    int(time.time()),
                    int(record.topic_created_at),
                    int(record.topic_id),
                    reason,
                    occurred_at,
                )
                if int(cursor.rowcount or 0) > 0:
                    inserted += 1

            conn.commit()
            return inserted
        finally:
            conn.close()

    def get_sensitive_title_records(
        self,
        topic_created_at: Optional[int] = None,
        topic_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[SensitiveTitleRecord]:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            where_clauses = []
            params = []

            if topic_created_at is not None:
                where_clauses.append("topic_created_at = ?")
                params.append(int(topic_created_at))
            if topic_id is not None:
                where_clauses.append("topic_id = ?")
                params.append(int(topic_id))
            if start_time is not None:
                where_clauses.append("occurred_at >= ?")
                params.append(int(start_time))
            if end_time is not None:
                where_clauses.append("occurred_at <= ?")
                params.append(int(end_time))

            where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            query = f"""
                SELECT TOP (?)
                    topic_created_at, topic_id, topic_name, old_topic,
                    candidate_titles_json, reason, risk_level, context_json, occurred_at
                FROM topic_sensitive_title_audit
                {where_sql}
                ORDER BY occurred_at DESC, id DESC
            """
            cursor.execute(query, limit, *params)
            
            results = []
            for row in cursor.fetchall():
                results.append(SensitiveTitleRecord(
                    topic_created_at=int(row[0]),
                    topic_id=int(row[1]),
                    topic_name=str(row[2]),
                    old_topic=str(row[3]),
                    candidate_titles=json.loads(row[4]) if row[4] else [],
                    reason=str(row[5]),
                    risk_level=str(row[6]),
                    context=json.loads(row[7]) if row[7] else {},
                    occurred_at=int(row[8])
                ))
            return results
        finally:
            conn.close()
