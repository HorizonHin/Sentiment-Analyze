# coding=utf-8


from __future__ import annotations

from datetime import datetime, timedelta
import os
import threading
import time
from pathlib import Path
from typing import Any

from SentimentAnalyzeServer.application.dataFetcherAppService import DataFetcherAppService
from SentimentAnalyzeServer.application.sentimentAnalyzeAppsService import SentimentAnalyzeAppService
from SentimentAnalyzeServer.application.topicAppService import TopicAppService


_DEFAULT_INTERVAL_SECONDS = 30 * 60


class Scheduled:
    def __init__(
        self,
        config_path: str | Path,
        storage: object,
        interval_seconds: int = _DEFAULT_INTERVAL_SECONDS,
        llm_max_workers: int = 32,
        data_fetcher_app_service: DataFetcherAppService | None = None,
        sentiment_app_service: SentimentAnalyzeAppService | None = None,
        topic_app_service: TopicAppService | None = None,
        run_immediately: bool = True,
    ) -> None:
        self.config_path = Path(config_path)
        self.interval_seconds = max(60, int(interval_seconds))
        self.storage = storage
        self.llm_max_workers = max(1, int(llm_max_workers))
        self.dataFetcher_app_service = data_fetcher_app_service or DataFetcherAppService(self.config_path, storage)
        self.sentiment_app_service = sentiment_app_service
        self.topic_app_service = topic_app_service
        self.run_immediately = bool(run_immediately)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> dict[str, Any]:
        result, _ = self.dataFetcher_app_service.crawl_and_save_news_data()
        return result

    def run_analyze_pending_once(self) -> dict[str, Any]:
        if self.sentiment_app_service is None:
            return {"success": False, "reason": "sentiment_service_not_configured"}

        end_time = datetime.now() - timedelta(minutes=30)
        return self.sentiment_app_service.analyze_pending_items_by_latest_time(
            start_time=None,
            end_time=end_time,
        )

    def run_refresh_topics_once(self) -> dict[str, Any]:
        if self.topic_app_service is None:
            return {"success": False, "reason": "topic_service_not_configured"}

        now = datetime.now()
        start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        topics = self.topic_app_service.recommend_and_cache_topics(
            start_time=start_time,
            end_time=now,
            top_n=10,
            cache_limit=15,
        )
        return {"success": True, "topic_count": len(topics)}

    def _loop(self) -> None:
        if not self.run_immediately:
            # Avoid running immediately on process start (useful for dev hot reload).
            if self._stop_event.wait(self.interval_seconds):
                return

        while not self._stop_event.is_set():
            started_at = time.time()
            try:
                self.run_analyze_pending_once()
            except Exception as exc:
                print(f"[dataAnalyzer] 补分析任务执行失败: {exc}")

            try:
                self.run_refresh_topics_once()
            except Exception as exc:
                print(f"[topicRefresher] 热门话题刷新失败: {exc}")

            try:
                self.run_once()
            except Exception as exc:
                print(f"[dataFetcher] 任务执行失败: {exc}")

            elapsed = time.time() - started_at
            wait_seconds = max(0, self.interval_seconds - elapsed)
            self._stop_event.wait(wait_seconds)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        if not self._thread or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()
        print(datetime.now().strftime("[%Y-%m-%d %H:%M]") +
            f"[Scheduled] 定时任务已启动，间隔: {self.interval_seconds // 60} 分钟"
        )

    def stop(self) -> None:
        self._stop_event.set()


def get_interval_seconds_from_env() -> int:
    raw = os.getenv("CRAWL_INTERVAL_MINUTES", "30").strip()
    try:
        minutes = max(1, int(raw))
    except ValueError:
        minutes = 30
    return minutes * 60
