from __future__ import annotations

import sqlite3
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
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS NewsItem (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_date TEXT NOT NULL,
                    crawl_time TEXT NOT NULL,
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
                    count INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(news_date, crawl_time, source_id, title)
                );

                CREATE TABLE IF NOT EXISTS Keyword (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_item_id INTEGER NOT NULL,
                    term TEXT NOT NULL,
                    importance REAL NOT NULL DEFAULT 0.0,
                    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS Entity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_item_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS rank_timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_item_id INTEGER NOT NULL,
                    timeline_time TEXT NOT NULL,
                    rank_value INTEGER,
                    FOREIGN KEY (news_item_id) REFERENCES NewsItem(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_news_date_crawl ON NewsItem(news_date, crawl_time);
                CREATE INDEX IF NOT EXISTS idx_news_source ON NewsItem(source_id);
                CREATE INDEX IF NOT EXISTS idx_keyword_news ON Keyword(news_item_id);
                CREATE INDEX IF NOT EXISTS idx_entity_news ON Entity(news_item_id);
                CREATE INDEX IF NOT EXISTS idx_timeline_news ON rank_timeline(news_item_id);
                """
            )

    def save_news_data(self, data: NewsData) -> bool:
        try:
            with self._get_connection() as conn:
                for source_id, news_list in data.items.items():
                    source_name = data.id_to_name.get(source_id, source_id)
                    for item in news_list:
                        conn.execute(
                            """
                            INSERT INTO NewsItem (
                                news_date, crawl_time, title, source_id, source_name, event_type,
                                summary, latest_rank, url, mobile_url, sentiment_polarity,
                                positive_ratio, negative_ratio, neutral_ratio,
                                optimism_score, trust_score, controversy_score, attention_score,
                                first_time, last_time, analyzed_time, count
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(news_date, crawl_time, source_id, title)
                            DO UPDATE SET
                                source_name = excluded.source_name,
                                event_type = excluded.event_type,
                                summary = excluded.summary,
                                latest_rank = excluded.latest_rank,
                                url = excluded.url,
                                mobile_url = excluded.mobile_url,
                                sentiment_polarity = excluded.sentiment_polarity,
                                positive_ratio = excluded.positive_ratio,
                                negative_ratio = excluded.negative_ratio,
                                neutral_ratio = excluded.neutral_ratio,
                                optimism_score = excluded.optimism_score,
                                trust_score = excluded.trust_score,
                                controversy_score = excluded.controversy_score,
                                attention_score = excluded.attention_score,
                                first_time = excluded.first_time,
                                last_time = excluded.last_time,
                                analyzed_time = excluded.analyzed_time,
                                count = excluded.count
                            """,
                            (
                                data.date,
                                data.crawl_time,
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
                                item.last_time,
                                item.analyzed_time,
                                item.count,
                            ),
                        )

                        row = conn.execute(
                            """
                            SELECT id FROM NewsItem
                            WHERE news_date = ? AND crawl_time = ? AND source_id = ? AND title = ?
                            """,
                            (data.date, data.crawl_time, item.source_id, item.title),
                        ).fetchone()
                        if row is None:
                            continue

                        news_item_id = int(row["id"])

                        conn.execute("DELETE FROM Keyword WHERE news_item_id = ?", (news_item_id,))
                        conn.execute("DELETE FROM Entity WHERE news_item_id = ?", (news_item_id,))
                        conn.execute("DELETE FROM rank_timeline WHERE news_item_id = ?", (news_item_id,))

                        for keyword in item.keywords:
                            conn.execute(
                                "INSERT INTO Keyword(news_item_id, term, importance) VALUES (?, ?, ?)",
                                (news_item_id, keyword.term, keyword.importance),
                            )

                        for entity in item.entities:
                            conn.execute(
                                "INSERT INTO Entity(news_item_id, name, entity_type) VALUES (?, ?, ?)",
                                (news_item_id, entity.name, entity.type),
                            )

                        for point in item.rank_timeline:
                            conn.execute(
                                "INSERT INTO rank_timeline(news_item_id, timeline_time, rank_value) VALUES (?, ?, ?)",
                                (
                                    news_item_id,
                                    str(point.get("time", "")),
                                    point.get("rank"),
                                ),
                            )

            return True
        except sqlite3.Error:
            return False

    def _load_snapshot(self, date_str: str, crawl_time: str) -> Optional[NewsData]:
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM NewsItem
                WHERE news_date = ? AND crawl_time = ?
                ORDER BY source_id, latest_rank ASC
                """,
                (date_str, crawl_time),
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
                    crawl_time=str(row["crawl_time"]),
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
                    count=int(row["count"]),
                    rank_timeline=timeline_by_news.get(news_item_id, []),
                )
                items.setdefault(source_id, []).append(item)

            return NewsData(
                date=date_str,
                crawl_time=crawl_time,
                items=items,
                id_to_name=id_to_name,
                failed_ids=[],
            )

    def get_today_all_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        date_str = date or datetime.now().strftime("%Y-%m-%d")
        with self._get_connection() as conn:
            crawl_times = [
                str(row["crawl_time"])
                for row in conn.execute(
                    "SELECT DISTINCT crawl_time FROM NewsItem WHERE news_date = ? ORDER BY crawl_time ASC",
                    (date_str,),
                ).fetchall()
            ]

        if not crawl_times:
            return None

        merged: Optional[NewsData] = None
        for crawl_time in crawl_times:
            snapshot = self._load_snapshot(date_str, crawl_time)
            if snapshot is None:
                continue
            merged = snapshot if merged is None else merged.merge_with(snapshot)

        return merged

    def get_latest_crawl_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        date_str = date or datetime.now().strftime("%Y-%m-%d")
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT MAX(crawl_time) AS crawl_time FROM NewsItem WHERE news_date = ?",
                (date_str,),
            ).fetchone()
        if row is None or row["crawl_time"] is None:
            return None
        return self._load_snapshot(date_str, str(row["crawl_time"]))

    def detect_new_titles(self, current_data: NewsData) -> Dict[str, Dict]:
        with self._get_connection() as conn:
            old_rows = conn.execute(
                """
                SELECT source_id, title
                FROM NewsItem
                WHERE news_date = ? AND crawl_time < ?
                """,
                (current_data.date, current_data.crawl_time),
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


def main() -> None:
    current_dir = Path(__file__).resolve().parent
    db_dir = current_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)

    db_path = db_dir / "news.db"
    SQLiteStorageBackend(db_path=str(db_path))
    print(f"SQLite database initialized: {db_path}")


if __name__ == "__main__":
    main()
