import os
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

try:
    import pyodbc
except ImportError:
    raise ImportError("pyodbc is required for MSSQL backend. Install with: pip install pyodbc")

from SentimentAnalyzeServer.domain.news.news import Entity, Keyword, NewsData, NewsItem, StorageBackend


class MSSQLStorageBackend(StorageBackend):
    """基于 SQL Server 的新闻数据存储后端。"""

    def __init__(
        self,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: str = "ODBC Driver 17 for SQL Server",
    ) -> None:
        self.server = server or os.getenv("MSSQL_SERVER", "localhost")
        self.database = database or os.getenv("MSSQL_DATABASE", "sentiment_analyze")
        self.username = username or os.getenv("MSSQL_USERNAME", "sa")
        self.password = password or os.getenv("MSSQL_PASSWORD", "")
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

        self._init_db()

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
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)

        text = str(value).strip().replace("T", " ")
        if len(text) == 10:
            date_part = self._parse_date_value(text)
            if date_part is not None:
                return datetime(date_part.year, date_part.month, date_part.day)

        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue

        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _to_date_str(self, value: Optional[object]) -> str:
        parsed = self._parse_date_value(value)
        if parsed is not None:
            return parsed.strftime("%Y-%m-%d")
        return "" if value is None else str(value)

    def _to_datetime_str(self, value: Optional[object]) -> str:
        parsed = self._parse_datetime_value(value)
        if parsed is not None:
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        return "" if value is None else str(value)

    def _init_db(self) -> None:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # 创建 NewsItem 表
            cursor.execute("""
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
            """)

            # 创建 Keyword 表
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Keyword')
                CREATE TABLE Keyword (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    news_item_id INT NOT NULL,
                    term NVARCHAR(500) NOT NULL,
                    create_time DATETIME2 DEFAULT GETDATE(),
                    importance FLOAT DEFAULT 0.0,
                    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
                );
            """)

            # 创建 Entity 表
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Entity')
                CREATE TABLE Entity (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    news_item_id INT NOT NULL,
                    name NVARCHAR(200) NOT NULL,
                    entity_type NVARCHAR(100) NOT NULL,
                    create_time DATETIME2 DEFAULT GETDATE(),
                    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
                );
            """)

            # 创建 rank_timeline 表
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rank_timeline')
                CREATE TABLE rank_timeline (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    news_item_id INT NOT NULL,
                    timeline_time DATETIME2 NOT NULL,
                    rank_value INT,
                    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
                );
            """)

            # 创建索引
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_news_date_last' AND object_id = OBJECT_ID('NewsItem'))
                CREATE INDEX idx_news_date_last ON NewsItem(source_id, title);
            """)

            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_news_source' AND object_id = OBJECT_ID('NewsItem'))
                CREATE INDEX idx_news_source ON NewsItem(source_id);
            """)

            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_keyword_news' AND object_id = OBJECT_ID('Keyword'))
                CREATE INDEX idx_keyword_news ON Keyword(news_item_id);
            """)

            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_entity_news' AND object_id = OBJECT_ID('Entity'))
                CREATE INDEX idx_entity_news ON Entity(news_item_id);
            """)

            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_timeline_news' AND object_id = OBJECT_ID('rank_timeline'))
                CREATE INDEX idx_timeline_news ON rank_timeline(news_item_id);
            """)

            conn.commit()
        except pyodbc.Error as e:
            conn.rollback()
            raise RuntimeError(f"MSSQL 数据库初始化失败: {e}") from e
        finally:
            conn.close()

    def _replace_keyword_and_entity(self, conn: pyodbc.Connection, valid_news: List[NewsItem]) -> None:
        cursor = conn.cursor()

        for item in valid_news:
            cursor.execute("DELETE FROM Keyword WHERE news_item_id = ?", int(item.id))
            cursor.execute("DELETE FROM Entity WHERE news_item_id = ?", int(item.id))

        for item in valid_news:
            create_time = self._parse_datetime_value(item.last_time) or datetime.now()
            for keyword in item.keywords:
                cursor.execute(
                    "INSERT INTO Keyword(news_item_id, term, create_time, importance) VALUES (?, ?, ?, ?)",
                    int(item.id),
                    keyword.term,
                    create_time,
                    keyword.importance,
                )

        for item in valid_news:
            create_time = self._parse_datetime_value(item.last_time) or datetime.now()
            for entity in item.entities:
                cursor.execute(
                    "INSERT INTO Entity(news_item_id, name, entity_type, create_time) VALUES (?, ?, ?, ?)",
                    int(item.id),
                    entity.name,
                    entity.type,
                    create_time,
                )

    def add_news_items(self, news_list: List[NewsItem]) -> List[NewsItem]:
        key_list = list(
            {
                (item.source_id, item.title)
                for item in news_list
                if item.source_id and item.title
            }
        )
        if not key_list:
            return []

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            for item in news_list:
                data_date = self._parse_date_value(item.first_time) or date.today()
                effective_last_time = self._parse_datetime_value(item.last_time) or datetime.now()
                first_time = self._parse_datetime_value(item.first_time) or effective_last_time
                analyzed_time = self._parse_datetime_value(item.analyzed_time)
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
                   

            # 查询并更新 item.id 和 last_time
            for item in news_list:
                cursor.execute(
                    "SELECT id, last_time FROM NewsItem WHERE source_id = ? AND title = ?",
                    item.source_id,
                    item.title,
                )
                row = cursor.fetchone()
                if row:
                    item.id = row[0]
                    item.last_time = self._to_datetime_str(row[1])

            # 删除旧的 rank_timeline
            news_ids = [int(item.id) for item in news_list if item.id and int(item.id) > 0]
            if news_ids:
                placeholders = ",".join("?" * len(news_ids))
                cursor.execute(f"DELETE FROM rank_timeline WHERE news_item_id IN ({placeholders})", news_ids)

            # 插入新的 rank_timeline
            for item in news_list:
                if item.id < 0:
                    continue
                for point in item.rank_timeline:
                    timeline_time = self._parse_datetime_value(point.get("time"))
                    if timeline_time is None:
                        timeline_time = self._parse_datetime_value(item.last_time) or datetime.now()
                    cursor.execute(
                        "INSERT INTO rank_timeline(news_item_id, timeline_time, rank_value) VALUES (?, ?, ?)",
                        int(item.id),
                        timeline_time,
                        point.get("rank"),
                    )

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
                    first_time = self._parse_datetime_value(item.first_time) or datetime.now()
                    last_time = self._parse_datetime_value(item.last_time) or datetime.now()
                    analyzed_time = self._parse_datetime_value(item.analyzed_time)
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

                news_ids = [int(item.id) for item in valid_news]
                placeholders = ",".join("?" * len(news_ids))
                cursor.execute(f"DELETE FROM rank_timeline WHERE news_item_id IN ({placeholders})", news_ids)

                for item in valid_news:
                    for point in item.rank_timeline:
                        timeline_time = self._parse_datetime_value(point.get("time"))
                        if timeline_time is None:
                            timeline_time = self._parse_datetime_value(item.last_time) or datetime.now()
                        cursor.execute(
                            "INSERT INTO rank_timeline(news_item_id, timeline_time, rank_value) VALUES (?, ?, ?)",
                            int(item.id),
                            timeline_time,
                            point.get("rank"),
                        )

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
                    last_time = self._parse_datetime_value(item.last_time) or datetime.now()
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

                news_ids = [int(item.id) for item in valid_news]
                placeholders = ",".join("?" * len(news_ids))
                cursor.execute(f"DELETE FROM rank_timeline WHERE news_item_id IN ({placeholders})", news_ids)

                for item in valid_news:
                    for point in item.rank_timeline:
                        timeline_time = self._parse_datetime_value(point.get("time"))
                        if timeline_time is None:
                            timeline_time = self._parse_datetime_value(item.last_time) or datetime.now()
                        cursor.execute(
                            "INSERT INTO rank_timeline(news_item_id, timeline_time, rank_value) VALUES (?, ?, ?)",
                            int(item.id),
                            timeline_time,
                            point.get("rank"),
                        )

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
        data = self._load_filtered_data(where_sql=where_sql, params=params)
        if data is None:
            return []

        items: List[NewsItem] = []
        for news_list in data.items.values():
            items.extend(news_list)
        return items

    def _load_filtered_data(self, where_sql: str, params: List) -> Optional[NewsData]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                SELECT * FROM NewsItem
                WHERE {where_sql}
                ORDER BY news_date ASC, last_time ASC, source_id, latest_rank ASC
            """, params)

            rows = cursor.fetchall()
            if not rows:
                return None

            row_ids = [int(row[0]) for row in rows]

            # 获取 keywords
            keywords_by_news: Dict[int, List[Keyword]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT news_item_id, term, importance FROM Keyword WHERE news_item_id IN ({placeholders}) ORDER BY id", row_ids)
                for keyword_row in cursor.fetchall():
                    news_item_id = int(keyword_row[0])
                    keywords_by_news.setdefault(news_item_id, []).append(
                        Keyword(term=str(keyword_row[1]), importance=float(keyword_row[2]))
                    )

            # 获取 entities
            entities_by_news: Dict[int, List[Entity]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT news_item_id, name, entity_type FROM Entity WHERE news_item_id IN ({placeholders}) ORDER BY id", row_ids)
                for entity_row in cursor.fetchall():
                    news_item_id = int(entity_row[0])
                    entities_by_news.setdefault(news_item_id, []).append(
                        Entity(name=str(entity_row[1]), type=str(entity_row[2]))
                    )

            # 获取 rank_timeline
            timeline_by_news: Dict[int, List[dict]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT news_item_id, timeline_time, rank_value FROM rank_timeline WHERE news_item_id IN ({placeholders}) ORDER BY id", row_ids)
                for timeline_row in cursor.fetchall():
                    news_item_id = int(timeline_row[0])
                    timeline_by_news.setdefault(news_item_id, []).append({
                        "time": self._to_datetime_str(timeline_row[1]),
                        "rank": timeline_row[2],
                    })

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
                    first_time=self._to_datetime_str(row[18]),
                    last_time=self._to_datetime_str(row[19]),
                    analyzed_time=self._to_datetime_str(row[20]) if row[20] is not None else None,
                    total_weigh=float(row[21]),
                    rank_timeline=timeline_by_news.get(news_item_id, []),
                )
                items.setdefault(source_id, []).append(item)

            latest_news_date = self._to_date_str(rows[-1][1]) if rows else ""
            latest_news_last_time = self._to_datetime_str(rows[-1][19]) if rows else ""

            return NewsData(
                date=latest_news_date,
                last_time=latest_news_last_time,
                items=items,
                id_to_name=id_to_name,
                failed_ids=[],
            )
        finally:
            conn.close()

    def get_latest_crawl_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        date_str = date or datetime.now().strftime("%Y-%m-%d")
        date_obj = self._parse_date_value(date_str) or datetime.now().date()
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

    def _load_snapshot(self, date_value: date, last_time: datetime) -> Optional[NewsData]:
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
                cursor.execute(f"SELECT news_item_id, term, importance FROM Keyword WHERE news_item_id IN ({placeholders})", row_ids)
                for keyword_row in cursor.fetchall():
                    news_item_id = int(keyword_row[0])
                    keywords_by_news.setdefault(news_item_id, []).append(
                        Keyword(term=str(keyword_row[1]), importance=float(keyword_row[2]))
                    )

            entities_by_news: Dict[int, List[Entity]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT news_item_id, name, entity_type FROM Entity WHERE news_item_id IN ({placeholders})", row_ids)
                for entity_row in cursor.fetchall():
                    news_item_id = int(entity_row[0])
                    entities_by_news.setdefault(news_item_id, []).append(
                        Entity(name=str(entity_row[1]), type=str(entity_row[2]))
                    )

            timeline_by_news: Dict[int, List[dict]] = {}
            if row_ids:
                placeholders = ",".join("?" * len(row_ids))
                cursor.execute(f"SELECT news_item_id, timeline_time, rank_value FROM rank_timeline WHERE news_item_id IN ({placeholders})", row_ids)
                for timeline_row in cursor.fetchall():
                    news_item_id = int(timeline_row[0])
                    timeline_by_news.setdefault(news_item_id, []).append({
                        "time": self._to_datetime_str(timeline_row[1]),
                        "rank": timeline_row[2],
                    })

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
                    first_time=self._to_datetime_str(row[18]),
                    last_time=self._to_datetime_str(row[19]),
                    analyzed_time=self._to_datetime_str(row[20]) if row[20] is not None else None,
                    total_weigh=float(row[21]),
                    rank_timeline=timeline_by_news.get(news_item_id, []),
                )
                items.setdefault(source_id, []).append(item)

            return NewsData(
                date=self._to_date_str(date_value),
                last_time=self._to_datetime_str(last_time),
                items=items,
                id_to_name=id_to_name,
                failed_ids=[],
            )
        finally:
            conn.close()

    def get_data_by_latest_crawl_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Optional[NewsData]:
        where_clauses: List[str] = []
        params: List = []

        start_dt = self._parse_datetime_value(start_time) if start_time else None
        end_dt = self._parse_datetime_value(end_time) if end_time else None

        if start_dt and end_dt:
            where_clauses.append("last_time >= ? AND last_time <= ?")
            params.extend([start_dt, end_dt])
        elif start_dt:
            where_clauses.append("last_time >= ?")
            params.append(start_dt)
        elif end_dt:
            where_clauses.append("last_time <= ?")
            params.append(end_dt)

        where_clauses.append("analyzed_time IS NOT NULL" if isAnalyzed else "analyzed_time IS NULL")

        where_sql = " AND ".join(where_clauses)
        return self._load_filtered_data(where_sql, params)

    def get_data_by_first_time_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Optional[NewsData]:
        where_clauses: List[str] = []
        params: List = []

        start_dt = self._parse_datetime_value(start_time) if start_time else None
        end_dt = self._parse_datetime_value(end_time) if end_time else None

        if start_dt and end_dt:
            if start_dt <= end_dt:
                where_clauses.append("first_time >= ? AND first_time <= ?")
                params.extend([start_dt, end_dt])
            else:
                where_clauses.append("(first_time >= ? OR first_time <= ?)")
                params.extend([start_dt, end_dt])
        elif start_dt:
            where_clauses.append("first_time >= ?")
            params.append(start_dt)
        elif end_dt:
            where_clauses.append("first_time <= ?")
            params.append(end_dt)

        where_clauses.append("analyzed_time IS NOT NULL" if isAnalyzed else "analyzed_time IS NULL")

        where_sql = " AND ".join(where_clauses)
        return self._load_filtered_data(where_sql, params)

    def detect_new_titles(self, current_data: NewsData) -> Dict[str, Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            news_date = self._parse_date_value(current_data.date)
            last_time = self._parse_datetime_value(current_data.last_time)
            if news_date is None or last_time is None:
                return {}

            cursor.execute("""
                SELECT source_id, title FROM NewsItem
                WHERE news_date = ? AND last_time < ?
            """, news_date, last_time)

            old_rows = cursor.fetchall()
            seen_titles = {(str(row[0]), str(row[1])) for row in old_rows}
            new_titles: Dict[str, Dict] = {}

            for source_id, news_list in current_data.items.items():
                for item in news_list:
                    key = (source_id, item.title)
                    if key in seen_titles:
                        continue
                    new_titles.setdefault(source_id, {})[item.title] = item.to_dict()

            return new_titles
        finally:
            conn.close()

    def is_first_crawl_today(self, date: Optional[str] = None) -> bool:
        date_str = date or datetime.now().strftime("%Y-%m-%d")
        date_obj = self._parse_date_value(date_str) or datetime.now().date()
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

        threshold = date.today() - timedelta(days=retention_days)
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
