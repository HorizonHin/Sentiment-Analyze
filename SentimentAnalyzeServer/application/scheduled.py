# coding=utf-8


from __future__ import annotations

from datetime import datetime, timedelta
import threading
import time
from pathlib import Path
from typing import Any

from SentimentAnalyzeServer.application.common import CommonThreadPool
from SentimentAnalyzeServer.application.dataFetcherAppService import DataFetcherAppService
from SentimentAnalyzeServer.application.sentimentAnalyzeAppsService import SentimentAnalyzeAppService
from SentimentAnalyzeServer.application.topicAppService import TopicAppService


_DEFAULT_INTERVAL_SECONDS = 30 * 60
_TOPIC_LOOKBACK_MULTIPLIER = 12.4


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
    ) -> None:
        self.config_path = Path(config_path)
        self.interval_seconds = max(60, int(interval_seconds))
        self.storage = storage
        self.llm_max_workers = max(1, int(llm_max_workers))
        self.dataFetcher_app_service = data_fetcher_app_service or DataFetcherAppService(self.config_path, storage)
        self.sentiment_app_service = sentiment_app_service
        self.topic_app_service = topic_app_service
        self.common_thread_pool = CommonThreadPool()

        self.system_dir = self.config_path.parent / "system"
        self.system_dir.mkdir(parents=True, exist_ok=True)
        self.last_run_file = self.system_dir / "last_scheduler_run.txt"

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()

    def start(self) -> bool:
        """Start scheduler loop in a daemon thread, return True if started now."""
        with self._thread_lock:
            if self._thread is not None and self._thread.is_alive():
                return False

            self._stop_event.clear()
            self._thread = threading.Thread(target=self._loop, name="scheduled-worker", daemon=True)
            self._thread.start()
            return True

    def stop(self, timeout: float = 10.0) -> bool:
        """Stop scheduler loop, return True when worker is fully stopped."""
        with self._thread_lock:
            thread = self._thread
            if thread is None:
                return True

            self._stop_event.set()

        thread.join(timeout=timeout)

        with self._thread_lock:
            stopped = not thread.is_alive()
            if stopped and self._thread is thread:
                self._thread = None
            return stopped

    def is_running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def fetch_and_store_news_data(self) -> dict[str, Any]:
        result, _ = self.dataFetcher_app_service.crawl_and_save_news_data()
        return result

    def run_analyze_pending_once(self) -> dict[str, Any]:
        if self.sentiment_app_service is None:
            return {"success": False, "reason": "sentiment_service_not_configured"}

        end_time = datetime.now() - timedelta(seconds=self.interval_seconds)
        return self.sentiment_app_service.analyze_pending_items_by_latest_time(
            start_time=None,
            end_time=end_time,
        )

    def run_refresh_topics_once(self) -> dict[str, Any]:
        if self.topic_app_service is None:
            return {"success": False, "reason": "topic_service_not_configured"}

        lookback_seconds = self.interval_seconds * _TOPIC_LOOKBACK_MULTIPLIER
        now = datetime.now() - timedelta(seconds=lookback_seconds)
        start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        topics = self.topic_app_service.recommend_and_cache_topics(
            start_time=start_time,
            end_time=now,
            top_n=10,
            cache_limit=15,
        )
        return {"success": True, "topic_count": len(topics)}

    def _read_last_completed_time(self) -> datetime | None:
        if not self.last_run_file.exists():
            return None

        try:
            raw = self.last_run_file.read_text(encoding="utf-8").strip()
        except OSError:
            return None

        if not raw:
            return None

        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None

    def _write_last_completed_time(self, completed_at: datetime) -> None:
        # Keep a single value in the file: the latest completed time.
        self.last_run_file.write_text(completed_at.isoformat(timespec="seconds"), encoding="utf-8")

    def _wait_until_next_run(self) -> bool:
        """Return True when caller should stop loop, False when cycle can proceed."""
        now = datetime.now()
        last_completed = self._read_last_completed_time()

        if last_completed is None:
            return False

        elapsed = (now - last_completed).total_seconds()
        if elapsed >= self.interval_seconds:
            return False

        wait_seconds = max(0.0, self.interval_seconds - elapsed)
        return self._stop_event.wait(wait_seconds)

    def _run_in_common_pool(self, fn, task_name: str) -> Any:
        future = self.common_thread_pool.submit(fn)
        try:
            return future.result()
        except Exception as exc:
            raise RuntimeError(f"{task_name} 执行失败: {exc}") from exc

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            if self._wait_until_next_run():
                return

            started_at = time.time()
            try:
                self._run_in_common_pool(self.run_analyze_pending_once, "dataAnalyzer")
            except Exception as exc:
                print(f"[dataAnalyzer] 补分析任务执行失败: {exc}")

            try:
                self._run_in_common_pool(self.run_refresh_topics_once, "topicRefresher")
            except Exception as exc:
                print(f"[topicRefresher] 热门话题刷新失败: {exc}")

            try:
                self._run_in_common_pool(self.fetch_and_store_news_data, "dataFetcher")
            except Exception as exc:
                print(f"[dataFetcher] 任务执行失败: {exc}")

            try:
                self._write_last_completed_time(datetime.now())
            except OSError as exc:
                print(f"[scheduler] 写入上次完成时间失败: {exc}")

            elapsed = time.time() - started_at
            wait_seconds = max(0, self.interval_seconds - elapsed)
            self._stop_event.wait(wait_seconds)


def get_interval_seconds_from_config(config: dict[str, Any] | None) -> int:
    scheduler_config = ((config or {}).get("scheduler") or {})
    raw = str(scheduler_config.get("crawl_interval_minutes", "30")).strip()
    try:
        minutes = max(1, int(raw))
    except ValueError:
        minutes = 30
    return minutes * 60
