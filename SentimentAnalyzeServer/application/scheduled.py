# coding=utf-8


from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

from SentimentAnalyzeServer.application.dataFetcherAppService import DataFetcherAppService


_DEFAULT_INTERVAL_SECONDS = 30 * 60


class Scheduled:
    def __init__(
        self,
        config_path: str | Path,
        storage: object,
        interval_seconds: int = _DEFAULT_INTERVAL_SECONDS,
        llm_max_workers: int = 32,
        data_fetcher_app_service: DataFetcherAppService | None = None,
    ) -> None:
        self.config_path = Path(config_path)
        self.interval_seconds = max(60, int(interval_seconds))
        self.storage = storage
        self.dataFetcher_app_service = data_fetcher_app_service or DataFetcherAppService(self.config_path, storage)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> dict[str, Any]:
        result, _ = self.dataFetcher_app_service.crawl_and_save_news_data()
        return result

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            started_at = time.time()
            try:
                self.run_once()
            except Exception as exc:
                print(f"[ScheduledCrawler] 任务执行失败: {exc}")

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
        print(
            f"[ScheduledCrawler] 定时任务已启动，间隔: {self.interval_seconds // 60} 分钟"
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
