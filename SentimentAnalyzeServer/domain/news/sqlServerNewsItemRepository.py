import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

try:
    import pyodbc
except ImportError:
    raise ImportError("pyodbc is required for MSSQL backend. Install with: pip install pyodbc")

from SentimentAnalyzeServer.domain.news.news import (
    Entity,
    Keyword,
    NewsData,
    NewsItem,
    NewsItemRepository,
    RankTimelineEntry,
)

logger = logging.getLogger(__name__)

class SqlServerNewsItemRepository(NewsItemRepository):
    """基于 SQL Server 的新闻数据存储后端。"""

    _NEWSITEM_SELECT_COLUMNS = (
        "id, news_date, title, source_id, source_name, event_type, summary, "
        "latest_rank, url, mobile_url, sentiment_polarity, positive_ratio, negative_ratio, neutral_ratio, "
        "optimism_score, trust_score, controversy_score, attention_score, first_time, last_time, analyzed_time, total_weigh"
    )

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

        # 构建连接字符串
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

        # 启用连接池
        pyodbc.pooling = True

    def _get_connection(self) -> pyodbc.Connection:
        conn = pyodbc.connect(self.connection_string, timeout=10)
        # conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        # # SQL Server NVARCHAR/NCHAR is wide-char data encoded as UTF-16LE.
        # conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-16le')
        # conn.setencoding(encoding='utf-8')
        return conn

    def _to_timestamp(self, value: Optional[object], fallback_now: bool = False) -> Optional[int]:
        if value is None:
            if not fallback_now:
                return None
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

    def _to_day_timestamp(self, value: Optional[object], fallback_today: bool = False) -> Optional[int]:
        if value is None:
            if not fallback_today:
                return None
            now_ts = int(time.time())
            return now_ts - (now_ts % 86400)
        if isinstance(value, int):
            ts = int(value)
            return ts - (ts % 86400)
        raise TypeError(f"timestamp must be int, got {type(value).__name__}")

    def _expect_db_datetime_or_none(self, value: object) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                return value.astimezone().replace(tzinfo=None)
            return value
        raise TypeError(f"analyzed_time must be datetime from DB, got {type(value).__name__}")

    def _to_date_str(self, value: Optional[object]) -> str:
        if value is None:
            return ""
        ts = self._to_timestamp(value)
        return time.strftime("%Y-%m-%d", time.gmtime(int(ts)))

    def _to_datetime_str(self, value: Optional[object]) -> str:
        if value is None:
            return ""
        ts = self._to_timestamp(value)
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(ts)))

    def _resolve_partition_first_time(self, news_first_time: Optional[int]) -> int:
        if news_first_time is not None:
            return int(news_first_time)
        return int(time.time()) - self.first_time_lookback_seconds

    def _normalize_timeline_point(self, point: object) -> Optional[Tuple[str, int]]:
        """兼容 tuple/list/dict 的 timeline 点，统一返回 (time, rank)。"""
        time_value = ""
        rank_value: Optional[object] = None

        if isinstance(point, dict):
            time_value = str(point.get("time", "")).strip()
            rank_value = point.get("rank")
        elif isinstance(point, (list, tuple)) and len(point) >= 2:
            time_value = str(point[0]).strip()
            rank_value = point[1]
        else:
            return None

        if not time_value:
            return None

        if rank_value is None:
            rank_int = 0
        else:
            try:
                rank_int = int(rank_value)
            except (TypeError, ValueError):
                return None

        return (time_value, rank_int)

    def _normalize_source_title_key(self, source_id: object, title: object) -> Tuple[str, str]:
        """标准化 (source_id, title) 键，避免空白差异导致重复判断不一致。"""
        normalized_source_id = str(source_id or "").strip()
        normalized_title = str(title or "").strip()
        return normalized_source_id, normalized_title

    def _is_unique_violation(self, error: pyodbc.Error) -> bool:
        text = str(error)
        return "2627" in text or "2601" in text or "UNIQUE KEY" in text.upper()

    def _upsert_rank_timeline_for_item(self, cursor: pyodbc.Cursor, item: NewsItem) -> None:
        """仅插入新的 rank_timeline 记录（id<=0）。"""
        news_item_id = int(item.id)
        if news_item_id <= 0:
            return
        news_first_time = self._to_timestamp(item.first_time) or self._to_timestamp(item.last_time, fallback_now=True)

        for point in item.rank_timeline_obj:
            # 只插入新数据（id <= 0）
            if point.id and int(point.id) > 0:
                continue

            timeline_time = self._to_timestamp(point.time)
            if timeline_time is None:
                timeline_time = self._to_timestamp(item.last_time, fallback_now=True)

            rank_value = point.rank if point.rank > 0 else None

            cursor.execute(
                "INSERT INTO rank_timeline(news_item_id, news_first_time, timeline_time, rank_value) OUTPUT INSERTED.id VALUES (?, ?, ?, ?)",
                news_item_id,
                news_first_time,
                timeline_time,
                rank_value,
            )
            inserted = cursor.fetchone()
            if inserted and inserted[0] is not None:
                point.id = int(inserted[0])

    def _replace_keyword_and_entity(self, conn: pyodbc.Connection, valid_news: List[NewsItem]) -> None:
        """覆盖式更新关键字和实体：先删除旧关联，再插入新关联。"""
        cursor = conn.cursor()

        for item in valid_news:
            if item.id is None or int(item.id) <= 0:
                continue
            news_first_time = self._to_timestamp(item.first_time) or self._to_timestamp(item.last_time, fallback_now=True)
            item_last_time = self._to_timestamp(item.last_time, fallback_now=True)
            
            # 1. 覆盖式更新：先删除该新闻项下已有的所有关键词
            cursor.execute(
                "DELETE FROM Keyword WHERE news_item_id = ? AND news_first_time = ?",
                int(item.id),
                news_first_time
            )
            
            # 2. 插入当前最新的关键词列表
            for keyword in item.keywords:
                term = str(keyword.term or "").strip()
                if not term:
                    continue
                keyword_weigh = float(keyword.weigh if keyword.weigh is not None else item.total_weigh)
                
                cursor.execute(
                    "INSERT INTO Keyword(news_item_id, news_first_time, last_time, term, importance, weigh) VALUES (?, ?, ?, ?, ?, ?)",
                    int(item.id),
                    news_first_time,
                    item_last_time,
                    term,
                    keyword.importance,
                    keyword_weigh,
                )

        for item in valid_news:
            if item.id is None or int(item.id) <= 0:
                continue
            news_first_time = self._to_timestamp(item.first_time) or self._to_timestamp(item.last_time, fallback_now=True)
            item_last_time = self._to_timestamp(item.last_time, fallback_now=True)
            
            # 1. 覆盖式更新：先删除该新闻项下已有的所有实体
            cursor.execute(
                "DELETE FROM Entity WHERE news_item_id = ? AND news_first_time = ?",
                int(item.id),
                news_first_time
            )
            
            # 2. 插入当前最新的实体列表
            for entity in item.entities:
                entity_name = str(entity.name or "").strip()
                entity_type = str(entity.type or "").strip()
                if not entity_name or not entity_type:
                    continue
                entity_weigh = float(entity.weigh if entity.weigh is not None else item.total_weigh)

                cursor.execute(
                    "INSERT INTO Entity(news_item_id, news_first_time, last_time, name, entity_type, weigh) VALUES (?, ?, ?, ?, ?, ?)",
                    int(item.id),
                    news_first_time,
                    item_last_time,
                    entity_name,
                    entity_type,
                    entity_weigh,
                )

    def _build_datetime_range_clause(
        self,
        column_name: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Tuple[str, List]:
        where_clauses: List[str] = []
        params: List = []
        converted_column = column_name

        start_ts = self._to_timestamp(start_time)
        end_ts = self._to_timestamp(end_time)

        if start_ts is not None and end_ts is not None:
            if start_ts <= end_ts:
                where_clauses.append(f"{converted_column} >= ? AND {converted_column} <= ?")
                params.extend([start_ts, end_ts])
            else:
                where_clauses.append(f"({converted_column} >= ? OR {converted_column} <= ?)")
                params.extend([start_ts, end_ts])
        elif start_ts is not None:
            where_clauses.append(f"{converted_column} >= ?")
            params.append(start_ts)
        elif end_ts is not None:
            where_clauses.append(f"{converted_column} <= ?")
            params.append(end_ts)

        if not where_clauses:
            return "1=1", params
        return " AND ".join(where_clauses), params

    def _build_normalized_datetime_text_range_clause(
        self,
        column_name: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Tuple[str, List, str]:
        where_sql, params = self._build_datetime_range_clause(
            column_name=column_name,
            start_time=start_time,
            end_time=end_time,
        )
        return where_sql, params, column_name

    @staticmethod
    def _build_key_pairs_values_clause(row_key_pairs: List[tuple[int, int]]) -> Tuple[str, List[int]]:
        values_sql = " UNION ALL ".join(["SELECT ? AS news_item_id, ? AS news_first_time"] * len(row_key_pairs))
        params: List[int] = []
        for news_item_id, news_first_time in row_key_pairs:
            params.extend([int(news_item_id), int(news_first_time)])
        return values_sql, params

    def _load_related_by_composite_keys(
        self,
        cursor: pyodbc.Cursor,
        row_key_pairs: List[tuple[int, int]],
    ) -> tuple[Dict[int, List[Keyword]], Dict[int, List[Entity]], Dict[int, List[RankTimelineEntry]]]:
        keywords_by_news: Dict[int, List[Keyword]] = {}
        entities_by_news: Dict[int, List[Entity]] = {}
        timeline_by_news: Dict[int, List[RankTimelineEntry]] = {}

        if not row_key_pairs:
            return keywords_by_news, entities_by_news, timeline_by_news

        values_sql, pair_params = self._build_key_pairs_values_clause(row_key_pairs)

        cursor.execute(
            f"""
            WITH kpair(news_item_id, news_first_time) AS ({values_sql})
            SELECT k.id, k.news_item_id, k.news_first_time, k.last_time, k.term, k.importance, k.weigh
            FROM Keyword k
            INNER JOIN kpair p
                ON k.news_item_id = p.news_item_id
               AND k.news_first_time = p.news_first_time
            ORDER BY k.id
            """,
            *pair_params,
        )
        for keyword_row in cursor.fetchall():
            news_item_id = int(keyword_row[1])
            keywords_by_news.setdefault(news_item_id, []).append(
                Keyword(
                    id=int(keyword_row[0]),
                    news_item_id=int(keyword_row[1]),
                    news_first_time=self._to_timestamp(keyword_row[2]),
                    last_time=self._to_timestamp(keyword_row[3]),
                    term=str(keyword_row[4]),
                    importance=float(keyword_row[5] if keyword_row[5] is not None else 0.0),
                    weigh=float(keyword_row[6] if keyword_row[6] is not None else 0.0),
                )
            )

        cursor.execute(
            f"""
            WITH epair(news_item_id, news_first_time) AS ({values_sql})
            SELECT e.id, e.news_item_id, e.news_first_time, e.last_time, e.name, e.entity_type, e.weigh
            FROM Entity e
            INNER JOIN epair p
                ON e.news_item_id = p.news_item_id
               AND e.news_first_time = p.news_first_time
            ORDER BY e.id
            """,
            *pair_params,
        )
        for entity_row in cursor.fetchall():
            news_item_id = int(entity_row[1])
            entities_by_news.setdefault(news_item_id, []).append(
                Entity(
                    id=int(entity_row[0]),
                    news_item_id=int(entity_row[1]),
                    news_first_time=self._to_timestamp(entity_row[2]),
                    last_time=self._to_timestamp(entity_row[3]),
                    name=str(entity_row[4]),
                    type=str(entity_row[5]),
                    weigh=float(entity_row[6] if entity_row[6] is not None else 0.0),
                )
            )

        cursor.execute(
            f"""
            WITH tpair(news_item_id, news_first_time) AS ({values_sql})
            SELECT t.id, t.news_item_id, t.news_first_time, t.timeline_time, t.rank_value
            FROM rank_timeline t
            INNER JOIN tpair p
                ON t.news_item_id = p.news_item_id
               AND t.news_first_time = p.news_first_time
            ORDER BY t.id
            """,
            *pair_params,
        )
        for timeline_row in cursor.fetchall():
            timeline_id = int(timeline_row[0])
            news_item_id = int(timeline_row[1])
            rank_value = timeline_row[4]
            try:
                rank_int = int(rank_value) if rank_value is not None else 0
            except (TypeError, ValueError):
                rank_int = 0
            timeline_by_news.setdefault(news_item_id, []).append(
                RankTimelineEntry(
                    id=timeline_id,
                    news_item_id=news_item_id,
                    news_first_time=self._to_timestamp(timeline_row[2]),
                    time=self._to_timestamp(timeline_row[3]),
                    rank=rank_int,
                )
            )

        return keywords_by_news, entities_by_news, timeline_by_news

    def get_keywords_by_last_time_range(
        self,
        news_first_time: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Keyword]:
        partition_first_time = self._resolve_partition_first_time(news_first_time)
        where_sql, params, normalized_last_time = self._build_normalized_datetime_text_range_clause(
            column_name="last_time",
            start_time=start_time,
            end_time=end_time,
        )
        where_sql = f"news_first_time >= ? AND ({where_sql})"
        params = [partition_first_time] + params

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT id, news_item_id, news_first_time, last_time, term, importance, weigh FROM Keyword WHERE {where_sql} ORDER BY {normalized_last_time} ASC, id ASC",
                *params,
            )
            result: List[Keyword] = []
            for row in cursor.fetchall():
                result.append(
                    Keyword(
                        id=int(row[0]),
                        news_item_id=int(row[1]),
                        news_first_time=self._to_timestamp(row[2]),
                        last_time=self._to_timestamp(row[3]),
                        term=str(row[4]),
                        importance=float(row[5] if row[5] is not None else 0.0),
                        weigh=float(row[6] if row[6] is not None else 0.0),
                    )
                )
            return result
        finally:
            conn.close()

    def get_entities_by_last_time_range(
        self,
        news_first_time: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[Entity]:
        partition_first_time = self._resolve_partition_first_time(news_first_time)
        where_sql, params, normalized_last_time = self._build_normalized_datetime_text_range_clause(
            column_name="last_time",
            start_time=start_time,
            end_time=end_time,
        )
        where_sql = f"news_first_time >= ? AND ({where_sql})"
        params = [partition_first_time] + params

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT id, news_item_id, news_first_time, last_time, name, entity_type, weigh FROM Entity WHERE {where_sql} ORDER BY {normalized_last_time} ASC, id ASC",
                *params,
            )
            result: List[Entity] = []
            for row in cursor.fetchall():
                result.append(
                    Entity(
                        id=int(row[0]),
                        news_item_id=int(row[1]),
                        news_first_time=self._to_timestamp(row[2]),
                        last_time=self._to_timestamp(row[3]),
                        name=str(row[4]),
                        type=str(row[5]),
                        weigh=float(row[6] if row[6] is not None else 0.0),
                    )
                )
            return result
        finally:
            conn.close()

    def get_news_list_by_keywords(
        self,
        keywords: List[Keyword],
        news_first_time: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[NewsItem]:
        if not keywords:
            return []

        key_pairs = {
            (int(keyword.news_item_id), int(keyword.news_first_time))
            for keyword in keywords
            if isinstance(keyword, Keyword)
            and keyword.news_item_id is not None
            and keyword.news_first_time is not None
            and int(keyword.news_item_id) > 0
        }
        if not key_pairs:
            return []

        return self._load_news_list_by_composite_keys(
            row_key_pairs=list(key_pairs),
            news_first_time=news_first_time,
            start_time=start_time,
            end_time=end_time,
        )

    def _load_news_list_by_composite_keys(
        self,
        row_key_pairs: List[tuple[int, int]],
        news_first_time: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[NewsItem]:
        if not row_key_pairs:
            return []

        where_clauses: List[str] = []
        params: List = []

        composite_conditions = []
        for news_item_id, row_news_first_time in row_key_pairs:
            composite_conditions.append("(id = ? AND first_time = ?)")
            params.extend([int(news_item_id), int(row_news_first_time)])
        where_clauses.append(f"({' OR '.join(composite_conditions)})")

        partition_first_time = self._resolve_partition_first_time(news_first_time)
        where_clauses.append("first_time >= ?")
        params.append(partition_first_time)

        time_where_sql, time_params = self._build_datetime_range_clause(
            column_name="first_time",
            start_time=start_time,
            end_time=end_time,
        )
        if time_where_sql != "1=1":
            where_clauses.append(time_where_sql)
            params.extend(time_params)

        where_sql = " AND ".join(where_clauses)
        items = self._load_filtered_data(where_sql=where_sql, params=params)
        return items if items is not None else []

    def get_news_list_by_entities(
        self,
        entities: List[Entity],
        news_first_time: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[NewsItem]:
        if not entities:
            return []

        key_pairs = {
            (int(entity.news_item_id), int(entity.news_first_time))
            for entity in entities
            if isinstance(entity, Entity)
            and entity.news_item_id is not None
            and entity.news_first_time is not None
            and int(entity.news_item_id) > 0
        }
        if not key_pairs:
            return []

        return self._load_news_list_by_composite_keys(
            row_key_pairs=list(key_pairs),
            news_first_time=news_first_time,
            start_time=start_time,
            end_time=end_time,
        )

    def add_followed_keyword(self, keyword_term: str) -> bool:
        term = str(keyword_term or "").strip()
        if not term:
            return False

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO Followed_Keywords(keyword_term, created_at) VALUES (?, ?)",
                term,
                int(time.time()),
            )
            conn.commit()
            return True
        except pyodbc.Error as e:
            conn.rollback()
            if self._is_unique_violation(e):
                return False
            raise
        finally:
            conn.close()

    def delete_followed_keyword(self, keyword_term: str) -> bool:
        term = str(keyword_term or "").strip()
        if not term:
            return False

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM Followed_Keywords WHERE keyword_term = ?",
                term,
            )
            conn.commit()
            return bool(cursor.rowcount and int(cursor.rowcount) > 0)
        finally:
            conn.close()

    def list_followed_keywords(self, limit: int = 1000) -> List[str]:
        safe_limit = max(1, int(limit or 1000))
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT TOP (?) keyword_term FROM Followed_Keywords ORDER BY created_at DESC, id DESC",
                safe_limit,
            )
            return [str(row[0]).strip() for row in cursor.fetchall() if row and row[0]]
        finally:
            conn.close()

    def add_news_items(self, news_list: List[NewsItem]) -> List[NewsItem]:
        unique_items_by_key: Dict[Tuple[str, str], NewsItem] = {}
        for item in news_list:
            source_id, title = self._normalize_source_title_key(item.source_id, item.title)
            if not source_id or not title:
                continue
            item.source_id = source_id
            item.title = title
            # 同批次出现重复 key 时，保留最后一个，避免批内插入冲突
            unique_items_by_key[(source_id, title)] = item

        if not unique_items_by_key:
            return []

        deduplicated_news_list = list(unique_items_by_key.values())
        key_list = list(unique_items_by_key.keys())

        existing_items = self.get_news_list_by_source_title_list(key_list, 0)
        existing_keys = {
            self._normalize_source_title_key(item.source_id, item.title)
            for item in existing_items
            if item.source_id and item.title
        }
        to_insert = [
            item
            for item in deduplicated_news_list
            if self._normalize_source_title_key(item.source_id, item.title) not in existing_keys
        ]

        if not to_insert:
            return existing_items

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            for item in to_insert:
                data_date = self._to_day_timestamp(item.first_time, fallback_today=True)
                effective_last_time = self._to_timestamp(item.last_time, fallback_now=True)
                first_time = self._to_timestamp(item.first_time) or effective_last_time
                analyzed_time = item.analyzed_time
                try:
                    cursor.execute("""
                        INSERT INTO NewsItem (
                            news_date, title, source_id, source_name, event_type,
                            summary, latest_rank, url, mobile_url, sentiment_polarity,
                            positive_ratio, negative_ratio, neutral_ratio,
                            optimism_score, trust_score, controversy_score, attention_score,
                            first_time, last_time, analyzed_time, total_weigh
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data_date,
                        item.title,
                        item.source_id,
                        item.source_name or item.source_id,
                        item.event_type,
                        item.summary,
                        item.latest_rank,
                        item.url,
                        item.mobile_url,
                        item.sentiment_polarity,
                        item.positive_ratio,
                        item.negative_ratio,
                        item.neutral_ratio,
                        item.optimism_score,
                        item.trust_score,
                        item.controversy_score,
                        item.attention_score,
                        first_time,
                        effective_last_time,
                        analyzed_time,
                        item.total_weigh,
                    ))
                except pyodbc.Error as item_error:
                    # 并发或竞态场景下，若已被其他事务插入则跳过，避免整批失败
                    error_text = str(item_error)
                    if "2627" in error_text or "2601" in error_text or "UNIQUE KEY" in error_text.upper():
                        continue
                    raise
                   

            # 查询并更新 item.id 和 last_time
            for item in deduplicated_news_list:
                item_first_time = self._to_timestamp(item.first_time) or self._to_timestamp(item.last_time, fallback_now=True)
                cursor.execute(
                    "SELECT id, first_time, last_time FROM NewsItem WHERE source_id = ? AND title = ? AND first_time = ?",
                    item.source_id,
                    item.title,
                    item_first_time,
                )
                row = cursor.fetchone()
                if row:
                    item.id = row[0]
                    item.first_time = self._to_timestamp(row[1])
                    item.last_time = self._to_timestamp(row[2])

            for item in deduplicated_news_list:
                self._upsert_rank_timeline_for_item(cursor, item)

            self._replace_keyword_and_entity(conn, deduplicated_news_list)

            conn.commit()
            return self.get_news_list_by_source_title_list(key_list, 0)
        except pyodbc.Error as e:
            conn.rollback()
            logger.error(f"添加新闻数据失败: {e}")
            return []
        finally:
            conn.close()

    def update_news_list(self, news_list: List[NewsItem]) -> bool:
        valid_news = [item for item in news_list if item.id is not None and int(item.id) > 0]
        if not valid_news:
            return False

        max_retries = 5
        base_retry_delay = 0.2

        for attempt in range(1, max_retries + 1):
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                for item in valid_news:
                    first_time = self._to_timestamp(item.first_time, fallback_now=True)
                    last_time = self._to_timestamp(item.last_time, fallback_now=True)
                    analyzed_time = item.analyzed_time
                    cursor.execute("""
                        UPDATE NewsItem SET
                            title = ?, source_id = ?, source_name = ?, event_type = ?,
                            summary = ?, latest_rank = ?, url = ?, mobile_url = ?,
                            sentiment_polarity = ?, positive_ratio = ?, negative_ratio = ?,
                            neutral_ratio = ?, optimism_score = ?, trust_score = ?,
                            controversy_score = ?, attention_score = ?,
                            last_time = ?, analyzed_time = ?, total_weigh = ?
                        WHERE id = ? AND first_time = ?
                    """, (
                        item.title,
                        item.source_id,
                        item.source_name,
                        item.event_type,
                        item.summary,
                        item.latest_rank,
                        item.url,
                        item.mobile_url,
                        item.sentiment_polarity,
                        item.positive_ratio,
                        item.negative_ratio,
                        item.neutral_ratio,
                        item.optimism_score,
                        item.trust_score,
                        item.controversy_score,
                        item.attention_score,
                        last_time,
                        analyzed_time,
                        item.total_weigh,
                        int(item.id),
                        first_time,
                    ))

                for item in valid_news:
                    self._upsert_rank_timeline_for_item(cursor, item)

                self._replace_keyword_and_entity(conn, valid_news)

                conn.commit()
                return True
            except pyodbc.DatabaseError as e:
                conn.rollback()
                err = str(e).lower()
                if "timeout" in err and attempt < max_retries:
                    wait_seconds = base_retry_delay * (2 ** (attempt - 1))
                    logger.warning(f"更新新闻列表遇到超时，重试 {attempt}/{max_retries}，等待 {wait_seconds:.2f}s")
                    time.sleep(wait_seconds)
                    continue
                logger.error(f"更新新闻列表失败: {e}")
                return False
            except pyodbc.Error as e:
                conn.rollback()
                logger.error(f"更新新闻列表失败: {e}")
                return False
            finally:
                conn.close()

        return False

    def update_crawled_news_list(self, news_list: List[NewsItem]) -> List[NewsItem]:
        valid_news = [item for item in news_list if item.id is not None and int(item.id) > 0]
        if not valid_news:
            return []

        key_list = list(
            {
                (item.source_id, item.title)
                for item in valid_news
                if item.source_id and item.title
            }
        )
        if not key_list:
            return []

        max_retries = 5
        base_retry_delay = 0.2

        for attempt in range(1, max_retries + 1):
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                for item in valid_news:
                    first_time = self._to_timestamp(item.first_time, fallback_now=True)
                    last_time = self._to_timestamp(item.last_time, fallback_now=True)
                    cursor.execute(
                        """
                        UPDATE NewsItem SET
                            source_name = ?, latest_rank = ?, url = ?, mobile_url = ?,
                            last_time = ?, total_weigh = ?
                        WHERE id = ? AND first_time = ?
                        """,
                        (
                            item.source_name,
                            item.latest_rank,
                            item.url,
                            item.mobile_url,
                            last_time,
                            item.total_weigh,
                            int(item.id),
                            first_time,
                        ),
                    )

                for item in valid_news:
                    self._upsert_rank_timeline_for_item(cursor, item)

                conn.commit()
                return self.get_news_list_by_source_title_list(key_list, 0)
            except pyodbc.DatabaseError as e:
                conn.rollback()
                err = str(e).lower()
                if "timeout" in err and attempt < max_retries:
                    wait_seconds = base_retry_delay * (2 ** (attempt - 1))
                    logger.warning(f"抓取更新遇到超时，重试 {attempt}/{max_retries}，等待 {wait_seconds:.2f}s")
                    time.sleep(wait_seconds)
                    continue
                logger.error(f"抓取更新失败: {e}")
                return []
            except pyodbc.Error as e:
                conn.rollback()
                logger.error(f"抓取更新失败: {e}")
                return []
            finally:
                conn.close()

        return []

    def get_news_list_by_source_title_list(
        self,
        source_title_list: List[tuple[str, str]],
        first_time: int,
    ) -> List[NewsItem]:
        if not source_title_list:
            return []

        first_time_ts = self._to_timestamp(first_time)
        if first_time_ts is None:
            return []

        where_conditions = []
        params = []
        for source_id, title in source_title_list:
            if not source_id or not title:
                continue
            where_conditions.append("(source_id = ? AND title = ?)")
            params.extend([source_id, title])

        if not where_conditions:
            return []

        where_sql = f"({' OR '.join(where_conditions)}) AND first_time >= ?"
        params.append(first_time_ts)
        items = self._load_filtered_data(where_sql=where_sql, params=params)
        return items if items is not None else []

    def get_news_items_by_url(self, url_list: List[str], first_time: int) -> List[NewsItem]:
        if not url_list:
            return []

        first_time_ts = self._to_timestamp(first_time)
        if first_time_ts is None:
            return []

        where_conditions = []
        params = []
        for url in url_list:
            if not url or not url.strip():
                continue
            where_conditions.append("(url = ? OR mobile_url = ?)")
            params.extend([url, url])

        if not where_conditions:
            return []

        where_sql = f"({' OR '.join(where_conditions)}) AND first_time >= ?"
        params.append(first_time_ts)
        items = self._load_filtered_data(where_sql=where_sql, params=params)
        return items if items is not None else []

    def _load_filtered_data(
        self,
        where_sql: str,
        params: List,
        order_by: str = "news_date ASC, last_time ASC, source_id, latest_rank ASC",
        top_n: Optional[int] = None,
    ) -> Optional[List[NewsItem]]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            top_clause = f"TOP ({int(top_n)}) " if top_n is not None else ""
            cursor.execute(
                f"""
                SELECT {top_clause}{self._NEWSITEM_SELECT_COLUMNS} FROM NewsItem
                WHERE {where_sql}
                ORDER BY {order_by}
                """,
                *params,
            )

            rows = cursor.fetchall()
            if not rows:
                return None

            row_key_pairs: List[tuple[int, int]] = []
            for row in rows:
                row_id = int(row[0])
                row_first_time = self._to_timestamp(row[18])
                if row_first_time is None:
                    continue
                row_key_pairs.append((row_id, int(row_first_time)))

            keywords_by_news, entities_by_news, timeline_by_news = self._load_related_by_composite_keys(
                cursor=cursor,
                row_key_pairs=row_key_pairs,
            )

            items: List[NewsItem] = []
            for row in rows:
                source_id = str(row[3])
                source_name = str(row[4])
                news_item_id = int(row[0])

                item = NewsItem(
                    id=news_item_id,
                    title=str(row[2]),
                    source_id=source_id,
                    source_name=source_name,
                    event_type=str(row[5]),
                    summary=str(row[6]),
                    entities=entities_by_news.get(news_item_id, []),
                    keywords=keywords_by_news.get(news_item_id, []),
                    latest_rank=int(row[7]),
                    url=str(row[8]),
                    mobile_url=str(row[9]),
                    sentiment_polarity=str(row[10]),
                    positive_ratio=float(row[11]),
                    negative_ratio=float(row[12]),
                    neutral_ratio=float(row[13]),
                    optimism_score=float(row[14]),
                    trust_score=float(row[15]),
                    controversy_score=float(row[16]),
                    attention_score=float(row[17]),
                    first_time=self._to_timestamp(row[18]),
                    last_time=self._to_timestamp(row[19]),
                    analyzed_time=self._expect_db_datetime_or_none(row[20]),
                    total_weigh=float(row[21]),
                    rank_timeline_obj=timeline_by_news.get(news_item_id, []),
                )
                items.append(item)

            return items
        finally:
            conn.close()
            
    def _load_snapshot(self, date_value: int, last_time: object) -> Optional[NewsData]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            next_day_value = int(date_value) + 24 * 60 * 60
            cursor.execute("""
                SELECT id, news_date, title, source_id, source_name, event_type, summary,
                       latest_rank, url, mobile_url, sentiment_polarity, positive_ratio, negative_ratio, neutral_ratio,
                       optimism_score, trust_score, controversy_score, attention_score, first_time, last_time, analyzed_time, total_weigh
                FROM NewsItem
                WHERE news_date = ? AND last_time = ? AND first_time >= ? AND first_time < ?
                ORDER BY source_id, latest_rank ASC
            """, date_value, last_time, date_value, next_day_value)

            rows = cursor.fetchall()
            if not rows:
                return None

            row_key_pairs: List[tuple[int, int]] = []
            for row in rows:
                row_id = int(row[0])
                row_first_time = self._to_timestamp(row[18])
                if row_first_time is None:
                    continue
                row_key_pairs.append((row_id, int(row_first_time)))

            keywords_by_news, entities_by_news, timeline_by_news = self._load_related_by_composite_keys(
                cursor=cursor,
                row_key_pairs=row_key_pairs,
            )

            items: Dict[str, List[NewsItem]] = {}
            id_to_name: Dict[str, str] = {}

            for row in rows:
                source_id = str(row[3])
                source_name = str(row[4])
                id_to_name[source_id] = source_name
                news_item_id = int(row[0])

                item = NewsItem(
                    id=news_item_id,
                    title=str(row[2]),
                    source_id=source_id,
                    source_name=source_name,
                    event_type=str(row[5]),
                    summary=str(row[6]),
                    entities=entities_by_news.get(news_item_id, []),
                    keywords=keywords_by_news.get(news_item_id, []),
                    latest_rank=int(row[7]),
                    url=str(row[8]),
                    mobile_url=str(row[9]),
                    sentiment_polarity=str(row[10]),
                    positive_ratio=float(row[11]),
                    negative_ratio=float(row[12]),
                    neutral_ratio=float(row[13]),
                    optimism_score=float(row[14]),
                    trust_score=float(row[15]),
                    controversy_score=float(row[16]),
                    attention_score=float(row[17]),
                    first_time=self._to_timestamp(row[18]),
                    last_time=self._to_timestamp(row[19]),
                    analyzed_time=self._expect_db_datetime_or_none(row[20]),
                    total_weigh=float(row[21]),
                    rank_timeline_obj=timeline_by_news.get(news_item_id, []),
                )
                items.setdefault(source_id, []).append(item)

            return NewsData(
                date=self._to_timestamp(date_value),
                last_time=self._to_timestamp(last_time),
                items=items,
                id_to_name=id_to_name,
                failed_ids=[],
            )
        finally:
            conn.close()

    def get_news_list_by_latest_batch(
        self,
        isAnalyzed: bool,
        first_time: int,
    ) -> Optional[List[NewsItem]]:
        """获取最新一批已分析（或未分析）的新闻，按 last_time DESC 排序，返回 TOP 500。
        
        Args:
            isAnalyzed: 是否已分析
            first_time: 分区键（下界），用于过滤分区
        
        Returns:
            按 last_time 降序排列的最新新闻，最多 500 条
        """
        partition_first_time = self._to_timestamp(first_time)
        if partition_first_time is None:
            raise ValueError("first_time is required for partition filtering")
        
        analyzed_filter = "analyzed_time IS NOT NULL" if isAnalyzed else "analyzed_time IS NULL"
        where_sql = f"first_time >= ? AND {analyzed_filter}"
        params = [partition_first_time]

        # 按 last_time 降序，返回最新的 500 条
        return self._load_filtered_data(
            where_sql=where_sql,
            params=params,
            order_by="last_time DESC, id DESC",
            top_n=500,
        )

    def get_news_list_by_first_time_range(
        self,
        isAnalyzed: bool,
        start_time: int,
        end_time: Optional[int] = None,
    ) -> Optional[List[NewsItem]]:
        time_where_sql, params = self._build_datetime_range_clause(
            column_name="first_time",
            start_time=start_time,
            end_time=end_time,
        )

        where_clauses: List[str] = []
        if time_where_sql != "1=1":
            where_clauses.append(time_where_sql)

        where_clauses.append("analyzed_time IS NOT NULL" if isAnalyzed else "analyzed_time IS NULL")

        where_sql = " AND ".join(where_clauses)
        return self._load_filtered_data(where_sql, params)

    def is_first_crawl_today(self, date: Optional[int] = None) -> bool:
        date_obj = self._to_day_timestamp(date, fallback_today=True)
        next_day_obj = int(date_obj) + 24 * 60 * 60
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(1) FROM NewsItem WHERE news_date = ? AND first_time >= ? AND first_time < ?",
                date_obj,
                date_obj,
                next_day_obj,
            )
            row = cursor.fetchone()
            return bool(row and int(row[0]) == 0)
        finally:
            conn.close()

    @property
    def backend_name(self) -> str:
        return "mssql"

    @property
    def supports_txt(self) -> bool:
        return False
