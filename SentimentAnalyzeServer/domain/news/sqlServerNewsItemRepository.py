import time
from datetime import date, datetime, timedelta
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
from SentimentAnalyzeServer.system.datetime_utils import datetime_to_timestamp, parse_datetime_value


class SqlServerNewsItemRepository(NewsItemRepository):
    """基于 SQL Server 的新闻数据存储后端。"""

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

    def _get_connection(self) -> pyodbc.Connection:
        conn = pyodbc.connect(self.connection_string, timeout=10)
        # conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        # # SQL Server NVARCHAR/NCHAR is wide-char data encoded as UTF-16LE.
        # conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-16le')
        # conn.setencoding(encoding='utf-8')
        return conn

    def _parse_date_value(self, value: Optional[object]) -> Optional[date]:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value

        text = str(value).strip()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue

        try:
            return datetime.fromisoformat(text.replace("T", " ")).date()
        except ValueError:
            return None

    def _parse_datetime_value(self, value: Optional[object]) -> Optional[datetime]:
        if value is None or value == "":
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime(value.year, value.month, value.day)

        parsed = parse_datetime_value(value)
        if parsed is not None:
            return parsed

        text = str(value).strip().replace("T", " ").rstrip(".")
        if len(text) == 10:
            date_part = self._parse_date_value(text)
            if date_part is not None:
                return datetime(date_part.year, date_part.month, date_part.day)
        return None

    def _to_timestamp(self, value: Optional[object], fallback_now: bool = False) -> Optional[int]:
        dt = self._parse_datetime_value(value)
        if dt is None:
            if not fallback_now:
                return None
            dt = datetime.utcnow()
        ts = datetime_to_timestamp(dt)
        return int(ts) if ts is not None else None

    def _to_day_timestamp(self, value: Optional[object], fallback_today: bool = False) -> Optional[int]:
        day = self._parse_date_value(value)
        if day is None:
            dt = self._parse_datetime_value(value)
            if dt is not None:
                day = dt.date()
        if day is None:
            if not fallback_today:
                return None
            day = datetime.utcnow().date()

        day_start = datetime(day.year, day.month, day.day)
        ts = datetime_to_timestamp(day_start)
        return int(ts) if ts is not None else None

    def _to_date_str(self, value: Optional[object]) -> str:
        parsed = self._parse_date_value(value)
        if parsed is None:
            parsed_dt = self._parse_datetime_value(value)
            parsed = parsed_dt.date() if parsed_dt is not None else None
        if parsed is not None:
            return parsed.strftime("%Y-%m-%d")
        return "" if value is None else str(value)

    def _to_datetime_str(self, value: Optional[object]) -> str:
        parsed = self._parse_datetime_value(value)
        if parsed is not None:
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        return "" if value is None else str(value)

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

        for point in item.rank_timeline_obj:
            # 只插入新数据（id <= 0）
            if point.id and int(point.id) > 0:
                continue

            timeline_time = self._to_timestamp(point.time)
            if timeline_time is None:
                timeline_time = self._to_timestamp(item.last_time, fallback_now=True)

            rank_value = point.rank if point.rank > 0 else None

            cursor.execute(
                "INSERT INTO rank_timeline(news_item_id, timeline_time, rank_value) OUTPUT INSERTED.id VALUES (?, ?, ?)",
                news_item_id,
                timeline_time,
                rank_value,
            )
            inserted = cursor.fetchone()
            if inserted and inserted[0] is not None:
                point.id = int(inserted[0])

    def _replace_keyword_and_entity(self, conn: pyodbc.Connection, valid_news: List[NewsItem]) -> None:
        """插入新数据；已有数据仅更新业务字段，不回写 first_time。"""
        cursor = conn.cursor()

        for item in valid_news:
            if item.id is None or int(item.id) <= 0:
                continue
            first_time = self._to_timestamp(item.first_time) or self._to_timestamp(item.last_time, fallback_now=True)
            for keyword in item.keywords:
                term = str(keyword.term or "").strip()
                if not term:
                    continue
                keyword_weigh = float(keyword.weigh if keyword.weigh is not None else item.total_weigh)
                if keyword.id and int(keyword.id) > 0:
                    cursor.execute(
                        "UPDATE Keyword SET weigh = ? WHERE id = ? AND news_item_id = ?",
                        keyword_weigh,
                        int(keyword.id),
                        int(item.id),
                    )
                    continue

                cursor.execute(
                    "UPDATE Keyword SET importance = ?, weigh = ? WHERE news_item_id = ? AND term = ?",
                    keyword.importance,
                    keyword_weigh,
                    int(item.id),
                    term,
                )
                if cursor.rowcount and int(cursor.rowcount) > 0:
                    continue

                try:
                    cursor.execute(
                        "INSERT INTO Keyword(news_item_id, term, first_time, importance, weigh) VALUES (?, ?, ?, ?, ?)",
                        int(item.id),
                        term,
                        first_time,
                        keyword.importance,
                        keyword_weigh,
                    )
                except pyodbc.Error as keyword_error:
                    if not self._is_unique_violation(keyword_error):
                        raise
                    cursor.execute(
                        "UPDATE Keyword SET importance = ?, weigh = ? WHERE news_item_id = ? AND term = ?",
                        keyword.importance,
                        keyword_weigh,
                        int(item.id),
                        term,
                    )

        for item in valid_news:
            if item.id is None or int(item.id) <= 0:
                continue
            first_time = self._to_timestamp(item.first_time) or self._to_timestamp(item.last_time, fallback_now=True)
            for entity in item.entities:
                entity_name = str(entity.name or "").strip()
                entity_type = str(entity.type or "").strip()
                if not entity_name or not entity_type:
                    continue
                entity_weigh = float(entity.weigh if entity.weigh is not None else item.total_weigh)
                if entity.id and int(entity.id) > 0:
                    cursor.execute(
                        "UPDATE Entity SET weigh = ? WHERE id = ? AND news_item_id = ?",
                        entity_weigh,
                        int(entity.id),
                        int(item.id),
                    )
                    continue

                cursor.execute(
                    "UPDATE Entity SET weigh = ? WHERE news_item_id = ? AND name = ? AND entity_type = ?",
                    entity_weigh,
                    int(item.id),
                    entity_name,
                    entity_type,
                )
                if cursor.rowcount and int(cursor.rowcount) > 0:
                    continue

                try:
                    cursor.execute(
                        "INSERT INTO Entity(news_item_id, name, entity_type, first_time, weigh) VALUES (?, ?, ?, ?, ?)",
                        int(item.id),
                        entity_name,
                        entity_type,
                        first_time,
                        entity_weigh,
                    )
                except pyodbc.Error as entity_error:
                    if not self._is_unique_violation(entity_error):
                        raise
                    cursor.execute(
                        "UPDATE Entity SET weigh = ? WHERE news_item_id = ? AND name = ? AND entity_type = ?",
                        entity_weigh,
                        int(item.id),
                        entity_name,
                        entity_type,
                    )

    def _build_datetime_range_clause(
        self,
        column_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Tuple[str, List]:
        where_clauses: List[str] = []
        params: List = []
        trimmed = f"LTRIM(RTRIM(CONVERT(NVARCHAR(64), {column_name})))"
        normalized_column = (
            f"CASE WHEN RIGHT({trimmed}, 1) = '.' "
            f"THEN LEFT({trimmed}, LEN({trimmed}) - 1) ELSE {trimmed} END"
        )
        converted_column = (
            f"COALESCE("
            f"TRY_CONVERT(BIGINT, {normalized_column}), "
            f"DATEDIFF_BIG(SECOND, '1970-01-01', TRY_CONVERT(DATETIME2, {normalized_column}))"
            f")"
        )

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
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Tuple[str, List, str]:
        """Build a timestamp range clause compatible with BIGINT and legacy datetime text."""
        trimmed = f"LTRIM(RTRIM(CONVERT(NVARCHAR(64), {column_name})))"
        normalized_column = (
            f"CASE WHEN RIGHT({trimmed}, 1) = '.' "
            f"THEN LEFT({trimmed}, LEN({trimmed}) - 1) ELSE {trimmed} END"
        )
        epoch_column = (
            f"COALESCE("
            f"TRY_CONVERT(BIGINT, {normalized_column}), "
            f"DATEDIFF_BIG(SECOND, '1970-01-01', TRY_CONVERT(DATETIME2, {normalized_column}))"
            f")"
        )

        start_ts = self._to_timestamp(start_time)
        end_ts = self._to_timestamp(end_time)

        where_clauses: List[str] = []
        params: List = []

        if start_ts is not None and end_ts is not None:
            if start_ts <= end_ts:
                where_clauses.append(f"{epoch_column} >= ? AND {epoch_column} <= ?")
                params.extend([start_ts, end_ts])
            else:
                where_clauses.append(f"({epoch_column} >= ? OR {epoch_column} <= ?)")
                params.extend([start_ts, end_ts])
        elif start_ts is not None:
            where_clauses.append(f"{epoch_column} >= ?")
            params.append(start_ts)
        elif end_ts is not None:
            where_clauses.append(f"{epoch_column} <= ?")
            params.append(end_ts)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        return where_sql, params, epoch_column

    def get_keywords_by_last_time_range(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Keyword]:
        where_sql, params, normalized_first_time = self._build_normalized_datetime_text_range_clause(
            column_name="first_time",
            start_time=start_time,
            end_time=end_time,
        )

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT id, term, importance, weigh FROM Keyword WHERE {where_sql} ORDER BY {normalized_first_time} ASC, id ASC",
                *params,
            )
            result: List[Keyword] = []
            for row in cursor.fetchall():
                result.append(
                    Keyword(
                        id=int(row[0]),
                        term=str(row[1]),
                        importance=float(row[2] if row[2] is not None else 0.0),
                        weigh=float(row[3] if row[3] is not None else 0.0),
                    )
                )
            return result
        finally:
            conn.close()

    def get_entities_by_last_time_range(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Entity]:
        where_sql, params, normalized_first_time = self._build_normalized_datetime_text_range_clause(
            column_name="first_time",
            start_time=start_time,
            end_time=end_time,
        )

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT id, name, entity_type, weigh FROM Entity WHERE {where_sql} ORDER BY {normalized_first_time} ASC, id ASC",
                *params,
            )
            result: List[Entity] = []
            for row in cursor.fetchall():
                result.append(
                    Entity(
                        id=int(row[0]),
                        name=str(row[1]),
                        type=str(row[2]),
                        weigh=float(row[3] if row[3] is not None else 0.0),
                    )
                )
            return result
        finally:
            conn.close()

    def get_news_list_by_keyword_ids(
        self,
        keyword_ids: List[int],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[NewsItem]:
        valid_ids = [int(i) for i in keyword_ids if int(i) > 0]
        if not valid_ids:
            return []

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            id_placeholders = ",".join("?" * len(valid_ids))
            cursor.execute(
                f"SELECT DISTINCT term FROM Keyword WHERE id IN ({id_placeholders})",
                *valid_ids,
            )
            terms = [str(row[0]).strip() for row in cursor.fetchall() if row and row[0] and str(row[0]).strip()]
        finally:
            conn.close()

        if not terms:
            return []

        term_placeholders = ",".join("?" * len(terms))
        where_clauses = [
            f"id IN (SELECT DISTINCT news_item_id FROM Keyword WHERE term IN ({term_placeholders}))"
        ]
        params: List = list(terms)

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

    def get_news_list_by_entity_ids(
        self,
        entity_ids: List[int],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[NewsItem]:
        valid_ids = [int(i) for i in entity_ids if int(i) > 0]
        if not valid_ids:
            return []

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            id_placeholders = ",".join("?" * len(valid_ids))
            cursor.execute(
                f"SELECT DISTINCT name FROM Entity WHERE id IN ({id_placeholders})",
                *valid_ids,
            )
            names = [str(row[0]).strip() for row in cursor.fetchall() if row and row[0] and str(row[0]).strip()]
        finally:
            conn.close()

        if not names:
            return []

        name_placeholders = ",".join("?" * len(names))
        where_clauses = [
            f"id IN (SELECT DISTINCT news_item_id FROM Entity WHERE name IN ({name_placeholders}))"
        ]
        params: List = list(names)

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

        existing_items = self.get_news_list_by_source_title_list(key_list)
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
                analyzed_time = self._to_timestamp(item.analyzed_time)
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
                cursor.execute(
                    "SELECT id, last_time FROM NewsItem WHERE source_id = ? AND title = ?",
                    item.source_id,
                    item.title,
                )
                row = cursor.fetchone()
                if row:
                    item.id = row[0]
                    item.last_time = self._parse_datetime_value(row[1])

            for item in deduplicated_news_list:
                self._upsert_rank_timeline_for_item(cursor, item)

            self._replace_keyword_and_entity(conn, deduplicated_news_list)

            conn.commit()
            return self.get_news_list_by_source_title_list(key_list)
        except pyodbc.Error as e:
            conn.rollback()
            print(f"添加新闻数据失败: {e}")
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
                    analyzed_time = self._to_timestamp(item.analyzed_time)
                    cursor.execute("""
                        UPDATE NewsItem SET
                            title = ?, source_id = ?, source_name = ?, event_type = ?,
                            summary = ?, latest_rank = ?, url = ?, mobile_url = ?,
                            sentiment_polarity = ?, positive_ratio = ?, negative_ratio = ?,
                            neutral_ratio = ?, optimism_score = ?, trust_score = ?,
                            controversy_score = ?, attention_score = ?, first_time = ?,
                            last_time = ?, analyzed_time = ?, total_weigh = ?
                        WHERE id = ?
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
                        first_time,
                        last_time,
                        analyzed_time,
                        item.total_weigh,
                        int(item.id),
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
                    print(f"更新新闻列表遇到超时，重试 {attempt}/{max_retries}，等待 {wait_seconds:.2f}s")
                    time.sleep(wait_seconds)
                    continue
                print(f"更新新闻列表失败: {e}")
                return False
            except pyodbc.Error as e:
                conn.rollback()
                print(f"更新新闻列表失败: {e}")
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
                    last_time = self._to_timestamp(item.last_time, fallback_now=True)
                    cursor.execute(
                        """
                        UPDATE NewsItem SET
                            source_name = ?, latest_rank = ?, url = ?, mobile_url = ?,
                            last_time = ?, total_weigh = ?
                        WHERE id = ?
                        """,
                        (
                            item.source_name,
                            item.latest_rank,
                            item.url,
                            item.mobile_url,
                            last_time,
                            item.total_weigh,
                            int(item.id),
                        ),
                    )

                for item in valid_news:
                    self._upsert_rank_timeline_for_item(cursor, item)

                conn.commit()
                return self.get_news_list_by_source_title_list(key_list)
            except pyodbc.DatabaseError as e:
                conn.rollback()
                err = str(e).lower()
                if "timeout" in err and attempt < max_retries:
                    wait_seconds = base_retry_delay * (2 ** (attempt - 1))
                    print(f"抓取更新遇到超时，重试 {attempt}/{max_retries}，等待 {wait_seconds:.2f}s")
                    time.sleep(wait_seconds)
                    continue
                print(f"抓取更新失败: {e}")
                return []
            except pyodbc.Error as e:
                conn.rollback()
                print(f"抓取更新失败: {e}")
                return []
            finally:
                conn.close()

        return []

    def get_news_list_by_source_title_list(self, source_title_list: List[tuple[str, str]]) -> List[NewsItem]:
        if not source_title_list:
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

        where_sql = " OR ".join(where_conditions)
        items = self._load_filtered_data(where_sql=where_sql, params=params)
        return items if items is not None else []

    def _load_filtered_data(self, where_sql: str, params: List) -> Optional[List[NewsItem]]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                SELECT * FROM NewsItem
                WHERE {where_sql}
                ORDER BY news_date ASC, last_time ASC, source_id, latest_rank ASC
            """, *params)

            rows = cursor.fetchall()
            if not rows:
                return None

            row_ids = [int(row[0]) for row in rows]

            # 获取 keywords
            keywords_by_news: Dict[int, List[Keyword]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT id, news_item_id, term, importance, weigh FROM Keyword WHERE news_item_id IN ({placeholders}) ORDER BY id", *row_ids)
                for keyword_row in cursor.fetchall():
                    news_item_id = int(keyword_row[1])
                    keywords_by_news.setdefault(news_item_id, []).append(
                        Keyword(id=int(keyword_row[0]), term=str(keyword_row[2]), importance=float(keyword_row[3]), weigh=float(keyword_row[4] if keyword_row[4] is not None else 0.0))
                    )

            # 获取 entities
            entities_by_news: Dict[int, List[Entity]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT id, news_item_id, name, entity_type, weigh FROM Entity WHERE news_item_id IN ({placeholders}) ORDER BY id", *row_ids)
                for entity_row in cursor.fetchall():
                    news_item_id = int(entity_row[1])
                    entities_by_news.setdefault(news_item_id, []).append(
                        Entity(id=int(entity_row[0]), name=str(entity_row[2]), type=str(entity_row[3]), weigh=float(entity_row[4] if entity_row[4] is not None else 0.0))
                    )

            # 获取 rank_timeline
            timeline_by_news: Dict[int, List[RankTimelineEntry]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT id, news_item_id, timeline_time, rank_value FROM rank_timeline WHERE news_item_id IN ({placeholders}) ORDER BY id", *row_ids)
                for timeline_row in cursor.fetchall():
                    timeline_id = int(timeline_row[0])
                    news_item_id = int(timeline_row[1])
                    rank_value = timeline_row[3]
                    try:
                        rank_int = int(rank_value) if rank_value is not None else 0
                    except (TypeError, ValueError):
                        rank_int = 0
                    timeline_by_news.setdefault(news_item_id, []).append(
                        RankTimelineEntry(id=timeline_id, time=self._parse_datetime_value(timeline_row[2]), rank=rank_int)
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
                    first_time=self._parse_datetime_value(row[18]),
                    last_time=self._parse_datetime_value(row[19]),
                    analyzed_time=self._parse_datetime_value(row[20]) if row[20] is not None else None,
                    total_weigh=float(row[21]),
                    rank_timeline_obj=timeline_by_news.get(news_item_id, []),
                )
                items.append(item)

            return items
        finally:
            conn.close()

    def get_latest_crawl_data(self, date: Optional[datetime] = None) -> Optional[NewsData]:
        date_obj = self._to_day_timestamp(date, fallback_today=True)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT MAX(last_time) FROM NewsItem WHERE news_date = ?", date_obj)
            row = cursor.fetchone()
            if row is None or row[0] is None:
                return None
            return self._load_snapshot(date_obj, row[0])
        finally:
            conn.close()

    def _load_snapshot(self, date_value: int, last_time: object) -> Optional[NewsData]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT * FROM NewsItem
                WHERE news_date = ? AND last_time = ?
                ORDER BY source_id, latest_rank ASC
            """, date_value, last_time)

            rows = cursor.fetchall()
            if not rows:
                return None

            row_ids = [int(row[0]) for row in rows]

            keywords_by_news: Dict[int, List[Keyword]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT id, news_item_id, term, importance, weigh FROM Keyword WHERE news_item_id IN ({placeholders})", *row_ids)
                for keyword_row in cursor.fetchall():
                    news_item_id = int(keyword_row[1])
                    keywords_by_news.setdefault(news_item_id, []).append(
                        Keyword(id=int(keyword_row[0]), term=str(keyword_row[2]), importance=float(keyword_row[3]), weigh=float(keyword_row[4] if keyword_row[4] is not None else 0.0))
                    )

            entities_by_news: Dict[int, List[Entity]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT id, news_item_id, name, entity_type, weigh FROM Entity WHERE news_item_id IN ({placeholders})", *row_ids)
                for entity_row in cursor.fetchall():
                    news_item_id = int(entity_row[1])
                    entities_by_news.setdefault(news_item_id, []).append(
                        Entity(id=int(entity_row[0]), name=str(entity_row[2]), type=str(entity_row[3]), weigh=float(entity_row[4] if entity_row[4] is not None else 0.0))
                    )

            timeline_by_news: Dict[int, List[RankTimelineEntry]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT id, news_item_id, timeline_time, rank_value FROM rank_timeline WHERE news_item_id IN ({placeholders}) ORDER BY id", *row_ids)
                for timeline_row in cursor.fetchall():
                    timeline_id = int(timeline_row[0])
                    news_item_id = int(timeline_row[1])
                    rank_value = timeline_row[3]
                    try:
                        rank_int = int(rank_value) if rank_value is not None else 0
                    except (TypeError, ValueError):
                        rank_int = 0
                    timeline_by_news.setdefault(news_item_id, []).append(
                        RankTimelineEntry(id=timeline_id, time=self._parse_datetime_value(timeline_row[2]), rank=rank_int)
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
                    first_time=self._parse_datetime_value(row[18]),
                    last_time=self._parse_datetime_value(row[19]),
                    analyzed_time=self._parse_datetime_value(row[20]) if row[20] is not None else None,
                    total_weigh=float(row[21]),
                    rank_timeline_obj=timeline_by_news.get(news_item_id, []),
                )
                items.setdefault(source_id, []).append(item)

            return NewsData(
                date=self._parse_datetime_value(date_value),
                last_time=self._parse_datetime_value(last_time),
                items=items,
                id_to_name=id_to_name,
                failed_ids=[],
            )
        finally:
            conn.close()

    def get_news_list_by_latest_crawl_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[List[NewsItem]]:
        time_where_sql, params = self._build_datetime_range_clause(
            column_name="last_time",
            start_time=start_time,
            end_time=end_time,
        )

        where_clauses: List[str] = []
        if time_where_sql != "1=1":
            where_clauses.append(time_where_sql)

        where_clauses.append("analyzed_time IS NOT NULL" if isAnalyzed else "analyzed_time IS NULL")

        where_sql = " AND ".join(where_clauses)
        return self._load_filtered_data(where_sql, params)

    def get_news_list_by_first_time_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
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

    def is_first_crawl_today(self, date: Optional[datetime] = None) -> bool:
        date_obj = self._to_day_timestamp(date, fallback_today=True)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(1) FROM NewsItem WHERE news_date = ?", date_obj)
            row = cursor.fetchone()
            return bool(row and int(row[0]) == 0)
        finally:
            conn.close()

    def cleanup(self) -> None:
        pass

    def cleanup_old_data(self, retention_days: int) -> int:
        if retention_days <= 0:
            return 0

        threshold_day = date.today() - timedelta(days=retention_days)
        threshold = self._to_day_timestamp(threshold_day)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT DISTINCT news_date FROM NewsItem
                WHERE news_date < ?
            """, threshold)

            old_dates = [self._to_date_str(row[0]) for row in cursor.fetchall()]
            cursor.execute("DELETE FROM NewsItem WHERE news_date < ?", threshold)
            conn.commit()

            return len(old_dates)
        finally:
            conn.close()

    @property
    def backend_name(self) -> str:
        return "mssql"

    @property
    def supports_txt(self) -> bool:
        return False
