# coding=utf-8


from __future__ import annotations

import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from SentimentAnalyzeServer.domain.crawler import DataFetcher
from SentimentAnalyzeServer.application.newsService import (
    _apply_llm_result,
    convert_crawl_results_and_save,
)
from SentimentAnalyzeServer.domain.llmAnalyzer.llmAnalyzer import LLMTitleAnalyzer
from SentimentAnalyzeServer.domain.news.news import NewsDomainService


_DEFAULT_INTERVAL_SECONDS = 30 * 60


class ScheduledCrawler:
    def __init__(
        self,
        config_path: str | Path,
        storage: object,
        interval_seconds: int = _DEFAULT_INTERVAL_SECONDS,
        analyze_interval_seconds: int = _DEFAULT_INTERVAL_SECONDS,
    ) -> None:
        self.config_path = Path(config_path)
        self.interval_seconds = max(60, int(interval_seconds))
        self.analyze_interval_seconds = max(60, int(analyze_interval_seconds))
        self.fetcher = DataFetcher()
        self.storage = storage
        self.analyzer = LLMTitleAnalyzer()
        self.domain_service = NewsDomainService(storage)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._analysis_thread: threading.Thread | None = None

    def _load_platforms(self) -> list[tuple[str, str]]:
        with self.config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        sources = (config.get("platforms") or {}).get("sources") or []
        ids: list[tuple[str, str]] = []
        for item in sources:
            if not isinstance(item, dict):
                continue
            platform_id = str(item.get("id", "")).strip()
            if not platform_id:
                continue
            name = str(item.get("name", platform_id)).strip() or platform_id
            ids.append((platform_id, name))

        return ids

    def run_once(self) -> dict[str, Any]:
        ids = self._load_platforms()
        if not ids:
            print("[ScheduledCrawler] 未在配置中找到可抓取平台")
            return {"success": False, "reason": "no_platforms"}

        print(f"[ScheduledCrawler] 开始抓取，平台数: {len(ids)}")
        results, id_to_name, failed_ids = self.fetcher.crawl_websites(ids)
        print(
            f"[ScheduledCrawler] 抓取完成，成功: {len(results)}，失败: {len(failed_ids)}"
        )

        now = datetime.now()
        crawl_date = now.strftime("%Y-%m-%d")
        crawl_time = now.strftime("%H:%M")
        current_data, saved = convert_crawl_results_and_save(
            results=results,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
            crawl_time=crawl_time,
            crawl_date=crawl_date,
            storage=self.storage,
        )

        if saved:
            all_items = []
            for news_list in current_data.items.values():
                all_items.extend(news_list)
            for item in all_items:
                if item.analyzed_time or item.sentiment_polarity or item.entities or item.keywords:
                    continue
                result = self.analyzer.analyze_title(item.title)
                _apply_llm_result(item, result)
                self.domain_service.save_news_data(current_data)

        return {
            "success": True,
            "platform_count": len(ids),
            "success_count": len(results),
            "failed_count": len(failed_ids),
            "failed_ids": failed_ids,
            "id_to_name": id_to_name,
        }

    def analyze_latest_once(self) -> dict[str, Any]:
        latest_data = self.domain_service.get_latest_crawl_data()
        if latest_data is None:
            print("[ScheduledCrawler] 无可分析的数据")
            return {"success": False, "reason": "no_data"}

        all_items = []
        for news_list in latest_data.items.values():
            all_items.extend(news_list)

        analyzed_count = 0
        for item in all_items:
            if item.analyzed_time or item.sentiment_polarity or item.entities or item.keywords:
                continue
            result = self.analyzer.analyze_title(item.title)
            _apply_llm_result(item, result)
            if self.domain_service.save_news_data(latest_data):
                analyzed_count += 1

        return {"success": True, "item_count": analyzed_count}

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

    def _analysis_loop(self) -> None:
        while not self._stop_event.is_set():
            started_at = time.time()
            try:
                self.analyze_latest_once()
            except Exception as exc:
                print(f"[ScheduledCrawler] 分析任务执行失败: {exc}")

            elapsed = time.time() - started_at
            wait_seconds = max(0, self.analyze_interval_seconds - elapsed)
            self._stop_event.wait(wait_seconds)

    def start(self) -> None:
        if self._thread and self._thread.is_alive() and self._analysis_thread and self._analysis_thread.is_alive():
            return

        self._stop_event.clear()
        if not self._thread or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()
        if not self._analysis_thread or not self._analysis_thread.is_alive():
            self._analysis_thread = threading.Thread(target=self._analysis_loop, daemon=True)
            self._analysis_thread.start()
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
