import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from SentimentAnalyzeServer.domain.news.news import Entity, Keyword, NewsData, NewsItem, StorageBackend


class SQLiteStorageBackend(StorageBackend):
    """基于 SQLite 的新闻数据存储后端。"""

    def __init__(self, db_path: Optional[str] = None) -> None:
        current_dir = Path(__file__).resolve().parent
        default_db_path = current_dir / "db" / "news.db"
        self.db_path = Path(db_path) if db_path else default_db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 10000")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS NewsItem (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_date TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    source_name TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    latest_rank INTEGER NOT NULL DEFAULT 0,
                    url TEXT NOT NULL DEFAULT '',
                    mobile_url TEXT NOT NULL DEFAULT '',
                    sentiment_polarity TEXT NOT NULL DEFAULT '',
                    positive_ratio REAL NOT NULL DEFAULT 0.0,
                    negative_ratio REAL NOT NULL DEFAULT 0.0,
                    neutral_ratio REAL NOT NULL DEFAULT 0.0,
                    optimism_score REAL NOT NULL DEFAULT 0.0,
                    trust_score REAL NOT NULL DEFAULT 0.0,
                    controversy_score REAL NOT NULL DEFAULT 0.0,
                    attention_score REAL NOT NULL DEFAULT 0.0,
                    first_time TEXT NOT NULL DEFAULT '',
                    last_time TEXT NOT NULL DEFAULT '',
                    analyzed_time TEXT,
                    total_weigh REAL NOT NULL DEFAULT 0.0,
                    UNIQUE(source_id, title)
                );

                CREATE TABLE IF NOT EXISTS Keyword (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_item_id INTEGER NOT NULL,
                    term TEXT NOT NULL,
                    create_time TEXT NOT NULL DEFAULT '',
                    importance REAL NOT NULL DEFAULT 0.0,
                    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS Entity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_item_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    create_time TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS rank_timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_item_id INTEGER NOT NULL,
                    timeline_time TEXT NOT NULL,
                    rank_value INTEGER,
                    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_news_date_last ON NewsItem(source_id, title);
                CREATE INDEX IF NOT EXISTS idx_news_source ON NewsItem(source_id);
                CREATE INDEX IF NOT EXISTS idx_keyword_news ON Keyword(news_item_id);
                CREATE INDEX IF NOT EXISTS idx_entity_news ON Entity(news_item_id);
                CREATE INDEX IF NOT EXISTS idx_timeline_news ON rank_timeline(news_item_id);
                """
            )

            columns = {
                str(row["name"]) for row in conn.execute("PRAGMA table_info(NewsItem)").fetchall()
            }

            keyword_columns = {
                str(row["name"]) for row in conn.execute("PRAGMA table_info(Keyword)").fetchall()
            }
            if "create_time" not in keyword_columns:
                conn.execute("ALTER TABLE Keyword ADD COLUMN create_time TEXT NOT NULL DEFAULT ''")

            entity_columns = {
                str(row["name"]) for row in conn.execute("PRAGMA table_info(Entity)").fetchall()
            }
            if "create_time" not in entity_columns:
                conn.execute("ALTER TABLE Entity ADD COLUMN create_time TEXT NOT NULL DEFAULT ''")

    def _replace_keyword_and_entity(self, conn: sqlite3.Connection, valid_news: List[NewsItem]) -> None:
        id_params = [(int(item.id),) for item in valid_news]
        conn.executemany("DELETE FROM Keyword WHERE news_item_id = ?", id_params)
        conn.executemany("DELETE FROM Entity WHERE news_item_id = ?", id_params)

        keyword_params: List[tuple] = []
        for item in valid_news:
            create_time = item.last_time or ""
            for keyword in item.keywords:
                keyword_params.append(
                    (
                        int(item.id),
                        keyword.term,
                        create_time,
                        keyword.importance,
                    )
                )

        if keyword_params:
            conn.executemany(
                "INSERT INTO Keyword(news_item_id, term, create_time, importance) VALUES (?, ?, ?, ?)",
                keyword_params,
            )

        entity_params: List[tuple] = []
        for item in valid_news:
            create_time = item.last_time or ""
            for entity in item.entities:
                entity_params.append(
                    (
                        int(item.id),
                        entity.name,
                        entity.type,
                        create_time,
                    )
                )

        if entity_params:
            conn.executemany(
                "INSERT INTO Entity(news_item_id, name, entity_type, create_time) VALUES (?, ?, ?, ?)",
                entity_params,
            )

    def add_news_data(self, data: NewsData) -> Optional[NewsData]:
        conn = self._get_connection()
        try:
            conn.execute("BEGIN")

            upsert_sql = """
            INSERT INTO NewsItem (
                news_date, title, source_id, source_name, event_type,
                summary, latest_rank, url, mobile_url, sentiment_polarity,
                positive_ratio, negative_ratio, neutral_ratio,
                optimism_score, trust_score, controversy_score, attention_score,
                first_time, last_time, analyzed_time, total_weigh
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id, title)
            DO UPDATE SET
                source_name = excluded.source_name,
                latest_rank = excluded.latest_rank,
                url = excluded.url,
                mobile_url = excluded.mobile_url,
                last_time = excluded.last_time
            """

            item_refs: List[tuple[str, str, NewsItem]] = []
            upsert_params_list: List[tuple] = []
            for source_id, news_list in data.items.items():
                source_name = data.id_to_name.get(source_id, source_id)
                for item in news_list:
                    effective_last_time = item.last_time or data.last_time
                    upsert_params_list.append(
                        (
                            data.date,
                            item.title,
                            item.source_id,
                            item.source_name or source_name,
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
                            item.first_time,
                            effective_last_time,
                            item.analyzed_time,
                            item.total_weigh,
                        )
                    )
                    item_refs.append((item.source_id, item.title, item))

            if not upsert_params_list:
                conn.commit()
                return data

            conn.executemany(upsert_sql, upsert_params_list)

            key_clauses: List[str] = []
            key_params: List[str] = []
            for source_id, title, _ in item_refs:
                key_clauses.append("(source_id = ? AND title = ?)")
                key_params.extend([source_id, title])

            select_sql = f"""
            SELECT id, source_id, title, last_time
            FROM NewsItem
            WHERE {" OR ".join(key_clauses)}
            """
            rows = conn.execute(select_sql, tuple(key_params)).fetchall()
            row_by_key = {
                (str(row["source_id"]), str(row["title"])): row
                for row in rows
            }

            news_item_ids: List[int] = []
            for source_id, title, item in item_refs:
                row = row_by_key.get((source_id, title))
                if row is None:
                    continue
                item.id = int(row["id"])
                item.last_time = str(row["last_time"])
                news_item_ids.append(item.id)

            unique_ids = list(dict.fromkeys(news_item_ids))
            if unique_ids:
                id_params = [(news_item_id,) for news_item_id in unique_ids]
                conn.executemany("DELETE FROM rank_timeline WHERE news_item_id = ?", id_params)

            timeline_params: List[tuple] = []
            for _, _, item in item_refs:
                if item.id < 0:
                    continue

                for point in item.rank_timeline:
                    timeline_params.append(
                        (
                            item.id,
                            str(point.get("time", "")),
                            point.get("rank"),
                        )
                    )

            if timeline_params:
                conn.executemany(
                    "INSERT INTO rank_timeline(news_item_id, timeline_time, rank_value) VALUES (?, ?, ?)",
                    timeline_params,
                )

            conn.commit()
            return data
        except sqlite3.Error:
            conn.rollback()
            return None
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
            try:
                conn.execute("BEGIN")

                update_sql = """
                UPDATE NewsItem
                SET
                    title = ?,
                    source_id = ?,
                    source_name = ?,
                    event_type = ?,
                    summary = ?,
                    latest_rank = ?,
                    url = ?,
                    mobile_url = ?,
                    sentiment_polarity = ?,
                    positive_ratio = ?,
                    negative_ratio = ?,
                    neutral_ratio = ?,
                    optimism_score = ?,
                    trust_score = ?,
                    controversy_score = ?,
                    attention_score = ?,
                    first_time = ?,
                    last_time = ?,
                    analyzed_time = ?,
                    total_weigh = ?
                WHERE id = ?
                """
                update_params = [
                    (
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
                        item.first_time,
                        item.last_time,
                        item.analyzed_time,
                        item.total_weigh,
                        item.id,
                    )
                    for item in valid_news
                ]
                conn.executemany(update_sql, update_params)

                id_params = [(int(item.id),) for item in valid_news]
                conn.executemany("DELETE FROM rank_timeline WHERE news_item_id = ?", id_params)
                self._replace_keyword_and_entity(conn, valid_news)

                timeline_params: List[tuple] = []
                for item in valid_news:
                    for point in item.rank_timeline:
                        timeline_params.append(
                            (
                                int(item.id),
                                str(point.get("time", "")),
                                point.get("rank"),
                            )
                        )

                if timeline_params:
                    conn.executemany(
                        "INSERT INTO rank_timeline(news_item_id, timeline_time, rank_value) VALUES (?, ?, ?)",
                        timeline_params,
                    )

                conn.commit()
                return True
            except sqlite3.OperationalError as e:
                conn.rollback()
                err = str(e).lower()
                if "database is locked" in err and attempt < max_retries:
                    wait_seconds = base_retry_delay * (2 ** (attempt - 1))
                    print(f"更新新闻列表遇到数据库锁，重试 {attempt}/{max_retries}，等待 {wait_seconds:.2f}s")
                    time.sleep(wait_seconds)
                    continue
                print(f"更新新闻列表失败: {e}")
                return False
            except sqlite3.Error as e:
                conn.rollback()
                print(f"更新新闻列表失败: {e}")
                return False
            finally:
                conn.close()

        return False

    def get_news_list_by_source_title_list(self, source_title_list: List[tuple[str, str]]) -> List[NewsItem]:
        if not source_title_list:
            return []

        key_clauses: List[str] = []
        key_params: List[str] = []
        for source_id, title in source_title_list:
            if not source_id or not title:
                continue
            key_clauses.append("(source_id = ? AND title = ?)")
            key_params.extend([source_id, title])

        if not key_clauses:
            return []

        where_sql = " OR ".join(key_clauses)
        data = self._load_filtered_data(where_sql=where_sql, params=key_params)
        if data is None:
            return []

        items: List[NewsItem] = []
        for news_list in data.items.values():
            items.extend(news_list)
        return items

    def _load_snapshot(self, date_str: str, last_time: str) -> Optional[NewsData]:
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM NewsItem
                WHERE news_date = ? AND last_time = ?
                ORDER BY source_id, latest_rank ASC
                """,
                (date_str, last_time),
            ).fetchall()

            if not rows:
                return None

            row_ids = [int(row["id"]) for row in rows]
            placeholders = ",".join("?" for _ in row_ids)

            keywords_by_news: Dict[int, List[Keyword]] = {}
            for keyword_row in conn.execute(
                f"SELECT news_item_id, term, importance FROM Keyword WHERE news_item_id IN ({placeholders}) ORDER BY id",
                row_ids,
            ).fetchall():
                news_item_id = int(keyword_row["news_item_id"])
                keywords_by_news.setdefault(news_item_id, []).append(
                    Keyword(term=str(keyword_row["term"]), importance=float(keyword_row["importance"]))
                )

            entities_by_news: Dict[int, List[Entity]] = {}
            for entity_row in conn.execute(
                f"SELECT news_item_id, name, entity_type FROM Entity WHERE news_item_id IN ({placeholders}) ORDER BY id",
                row_ids,
            ).fetchall():
                news_item_id = int(entity_row["news_item_id"])
                entities_by_news.setdefault(news_item_id, []).append(
                    Entity(name=str(entity_row["name"]), type=str(entity_row["entity_type"]))
                )

            timeline_by_news: Dict[int, List[dict]] = {}
            for timeline_row in conn.execute(
                f"SELECT news_item_id, timeline_time, rank_value FROM rank_timeline WHERE news_item_id IN ({placeholders}) ORDER BY id",
                row_ids,
            ).fetchall():
                news_item_id = int(timeline_row["news_item_id"])
                timeline_by_news.setdefault(news_item_id, []).append(
                    {
                        "time": str(timeline_row["timeline_time"]),
                        "rank": timeline_row["rank_value"],
                    }
                )

            items: Dict[str, List[NewsItem]] = {}
            id_to_name: Dict[str, str] = {}

            for row in rows:
                source_id = str(row["source_id"])
                source_name = str(row["source_name"])
                id_to_name[source_id] = source_name
                news_item_id = int(row["id"])

                item = NewsItem(
                    id=news_item_id,
                    title=str(row["title"]),
                    source_id=source_id,
                    source_name=source_name,
                    event_type=str(row["event_type"]),
                    summary=str(row["summary"]),
                    entities=entities_by_news.get(news_item_id, []),
                    keywords=keywords_by_news.get(news_item_id, []),
                    latest_rank=int(row["latest_rank"]),
                    url=str(row["url"]),
                    mobile_url=str(row["mobile_url"]),
                    sentiment_polarity=str(row["sentiment_polarity"]),
                    positive_ratio=float(row["positive_ratio"]),
                    negative_ratio=float(row["negative_ratio"]),
                    neutral_ratio=float(row["neutral_ratio"]),
                    optimism_score=float(row["optimism_score"]),
                    trust_score=float(row["trust_score"]),
                    controversy_score=float(row["controversy_score"]),
                    attention_score=float(row["attention_score"]),
                    first_time=str(row["first_time"]),
                    last_time=str(row["last_time"]),
                    analyzed_time=row["analyzed_time"],
                    total_weigh=float(row["total_weigh"]),
                    rank_timeline=timeline_by_news.get(news_item_id, []),
                )
                items.setdefault(source_id, []).append(item)

            return NewsData(
                date=date_str,
                last_time=last_time,
                items=items,
                id_to_name=id_to_name,
                failed_ids=[],
            )

    def _load_filtered_data(self, where_sql: str, params: List[str]) -> Optional[NewsData]:
        with self._get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM NewsItem
                WHERE {where_sql}
                ORDER BY news_date ASC, last_time ASC, source_id, latest_rank ASC
                """,
                tuple(params),
            ).fetchall()

            if not rows:
                return None

            row_ids = [int(row["id"]) for row in rows]
            placeholders = ",".join("?" for _ in row_ids)

            keywords_by_news: Dict[int, List[Keyword]] = {}
            for keyword_row in conn.execute(
                f"SELECT news_item_id, term, importance FROM Keyword WHERE news_item_id IN ({placeholders}) ORDER BY id",
                row_ids,
            ).fetchall():
                news_item_id = int(keyword_row["news_item_id"])
                keywords_by_news.setdefault(news_item_id, []).append(
                    Keyword(term=str(keyword_row["term"]), importance=float(keyword_row["importance"]))
                )

            entities_by_news: Dict[int, List[Entity]] = {}
            for entity_row in conn.execute(
                f"SELECT news_item_id, name, entity_type FROM Entity WHERE news_item_id IN ({placeholders}) ORDER BY id",
                row_ids,
            ).fetchall():
                news_item_id = int(entity_row["news_item_id"])
                entities_by_news.setdefault(news_item_id, []).append(
                    Entity(name=str(entity_row["name"]), type=str(entity_row["entity_type"]))
                )

            timeline_by_news: Dict[int, List[dict]] = {}
            for timeline_row in conn.execute(
                f"SELECT news_item_id, timeline_time, rank_value FROM rank_timeline WHERE news_item_id IN ({placeholders}) ORDER BY id",
                row_ids,
            ).fetchall():
                news_item_id = int(timeline_row["news_item_id"])
                timeline_by_news.setdefault(news_item_id, []).append(
                    {
                        "time": str(timeline_row["timeline_time"]),
                        "rank": timeline_row["rank_value"],
                    }
                )

            items: Dict[str, List[NewsItem]] = {}
            id_to_name: Dict[str, str] = {}
            latest_news_date = ""
            latest_news_last_time = ""

            for row in rows:
                source_id = str(row["source_id"])
                source_name = str(row["source_name"])
                id_to_name[source_id] = source_name
                news_item_id = int(row["id"])
                latest_news_date = str(row["news_date"])
                latest_news_last_time = str(row["last_time"])

                item = NewsItem(
                    id=news_item_id,
                    title=str(row["title"]),
                    source_id=source_id,
                    source_name=source_name,
                    event_type=str(row["event_type"]),
                    summary=str(row["summary"]),
                    entities=entities_by_news.get(news_item_id, []),
                    keywords=keywords_by_news.get(news_item_id, []),
                    latest_rank=int(row["latest_rank"]),
                    url=str(row["url"]),
                    mobile_url=str(row["mobile_url"]),
                    sentiment_polarity=str(row["sentiment_polarity"]),
                    positive_ratio=float(row["positive_ratio"]),
                    negative_ratio=float(row["negative_ratio"]),
                    neutral_ratio=float(row["neutral_ratio"]),
                    optimism_score=float(row["optimism_score"]),
                    trust_score=float(row["trust_score"]),
                    controversy_score=float(row["controversy_score"]),
                    attention_score=float(row["attention_score"]),
                    first_time=str(row["first_time"]),
                    last_time=str(row["last_time"]),
                    analyzed_time=row["analyzed_time"],
                    total_weigh=float(row["total_weigh"]),
                    rank_timeline=timeline_by_news.get(news_item_id, []),
                )
                items.setdefault(source_id, []).append(item)

            return NewsData(
                date=latest_news_date,
                last_time=latest_news_last_time,
                items=items,
                id_to_name=id_to_name,
                failed_ids=[],
            )

    def get_latest_crawl_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        date_str = date or datetime.now().strftime("%Y-%m-%d")
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT MAX(last_time) AS last_time FROM NewsItem WHERE news_date = ?",
                (date_str,),
            ).fetchone()
        if row is None or row["last_time"] is None:
            return None
        return self._load_snapshot(date_str, str(row["last_time"]))

    def get_data_by_latest_crawl_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Optional[NewsData]:
        where_clauses: List[str] = []
        params: List[str] = []

        if start_time and end_time:
            where_clauses.append("last_time >= ? AND last_time <= ?")
            params.extend([start_time, end_time])
        elif start_time:
            where_clauses.append("last_time >= ?")
            params.append(start_time)
        elif end_time:
            where_clauses.append("last_time <= ?")
            params.append(end_time)

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
        params: List[str] = []

        if start_time and end_time:
            if start_time <= end_time:
                where_clauses.append("first_time >= ? AND first_time <= ?")
                params.extend([start_time, end_time])
            else:
                where_clauses.append("(first_time >= ? OR first_time <= ?)")
                params.extend([start_time, end_time])
        elif start_time:
            where_clauses.append("first_time >= ?")
            params.append(start_time)
        elif end_time:
            where_clauses.append("first_time <= ?")
            params.append(end_time)

        where_clauses.append("analyzed_time IS NOT NULL" if isAnalyzed else "analyzed_time IS NULL")

        where_sql = " AND ".join(where_clauses)
        return self._load_filtered_data(where_sql, params)

    def detect_new_titles(self, current_data: NewsData) -> Dict[str, Dict]:
        with self._get_connection() as conn:
            old_rows = conn.execute(
                """
                SELECT source_id, title
                FROM NewsItem
                WHERE news_date = ? AND last_time < ?
                """,
                (current_data.date, current_data.last_time),
            ).fetchall()

        seen_titles = {(str(row["source_id"]), str(row["title"])) for row in old_rows}
        new_titles: Dict[str, Dict] = {}

        for source_id, news_list in current_data.items.items():
            for item in news_list:
                key = (source_id, item.title)
                if key in seen_titles:
                    continue
                new_titles.setdefault(source_id, {})[item.title] = item.to_dict()

        return new_titles

    def is_first_crawl_today(self, date: Optional[str] = None) -> bool:
        date_str = date or datetime.now().strftime("%Y-%m-%d")
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(1) AS cnt FROM NewsItem WHERE news_date = ?",
                (date_str,),
            ).fetchone()
        return bool(row and int(row["cnt"]) == 0)

    def cleanup(self) -> None:
        return None

    def cleanup_old_data(self, retention_days: int) -> int:
        if retention_days <= 0:
            return 0

        threshold = (date.today() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        with self._get_connection() as conn:
            old_dates = [
                str(row["news_date"])
                for row in conn.execute(
                    "SELECT DISTINCT news_date FROM NewsItem WHERE news_date < ?",
                    (threshold,),
                ).fetchall()
            ]
            conn.execute("DELETE FROM NewsItem WHERE news_date < ?", (threshold,))

        return len(old_dates)

    @property
    def backend_name(self) -> str:
        return "sqlite"

    @property
    def supports_txt(self) -> bool:
        return False

# simple test to create the database file and tables
def main() -> None:
    current_dir = Path(__file__).resolve().parent
    db_dir = current_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)

    db_path = db_dir / "news.db"
    SQLiteStorageBackend(db_path=str(db_path))
    print(f"SQLite database initialized: {db_path}")


if __name__ == "__main__":
    main()
