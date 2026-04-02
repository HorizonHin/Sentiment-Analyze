from __future__ import annotations

import logging
import time
from typing import Any, Dict, List

from SentimentAnalyzeServer.system.infra import (
    EVENT_CRAWL_SAVED,
    EVENT_SENTIMENT_ANALYZED,
    EVENT_TOPIC_RANK_UPDATED,
    EventManager,
)
from SentimentAnalyzeServer.application.sentimentAnalyzeAppsService import SentimentAnalyzeAppService
from SentimentAnalyzeServer.application.topicAppService import TopicAppService
from SentimentAnalyzeServer.domain.news.news import NewsItem


logger = logging.getLogger(__name__)


_TOPIC_LOOKBACK_MULTIPLIER = 12.4


class WorkflowEventSubscribers:
    def __init__(
        self,
        sentiment_app_service: SentimentAnalyzeAppService,
        topic_app_service: TopicAppService,
        crawl_interval_seconds: int,
    ) -> None:
        self._is_registered = False
        self.event_manager = EventManager()
        self.sentiment_app_service = sentiment_app_service
        self.topic_app_service = topic_app_service
        self.crawl_interval_seconds = max(60, int(crawl_interval_seconds))

    def register(self) -> None:
        if self._is_registered:
            logger.warning("WorkflowEventSubscribers already registered, skipping.")
            return
        self.event_manager.subscribe(EVENT_CRAWL_SAVED, self._on_crawl_saved)
        self.event_manager.subscribe(EVENT_SENTIMENT_ANALYZED, self._on_sentiment_analyzed)
        self.event_manager.subscribe(EVENT_TOPIC_RANK_UPDATED, self._on_topic_rank_updated)
        self._is_registered = True
        
    def _on_crawl_saved(self, payload: Dict[str, Any]) -> None:
        saved_items = payload.get("saved_items", [])
        if not isinstance(saved_items, list) or not saved_items:
            return
        logger.info("处理爬取保存成功事件，准备分析新闻项。总项数=%s", len(saved_items))
        valid_items: List[NewsItem] = [item for item in saved_items if isinstance(item, NewsItem)]
        if not valid_items:
            logger.info("Crawl saved event received but no valid news items found in payload.")
        # 注意：即使没有任何有效数据被分析，也要调用analyze_and_update_news_items
        # 以发布事件触发后续流程（如话题推荐）。
        try:
            self.sentiment_app_service.analyze_and_update_news_items(valid_items)
        except Exception:
            logger.exception("analyze_and_update_news_items failed")

    def _on_sentiment_analyzed(self, payload: Dict[str, Any]) -> None:
        # 在情感分析完成后，尝试推荐和缓存一个动态窗口内的热点话题
        lookback_seconds = self.crawl_interval_seconds * _TOPIC_LOOKBACK_MULTIPLIER
        end_time = int(time.time())
        start_time = end_time - int(lookback_seconds)
        logger.info('处理情感分析完成事件，触发话题推荐。lookback_seconds=%s, start_time=%s, end_time=%s', lookback_seconds, start_time, end_time)
        try:
            self.topic_app_service.recommend_and_cache_topics(
                start_time=start_time,
                end_time=end_time,
            )
        except Exception:
            logger.exception("recommend_and_cache_topics failed")

    def _on_topic_rank_updated(self, payload: Dict[str, Any]) -> None:
        raw_limit = payload.get("cache_limit", 50)
        try:
            limit = max(1, min(200, int(raw_limit)))
        except (TypeError, ValueError):
            limit = 50

        try:
            self.topic_app_service.backfill_missing_llm_titles(limit=limit)
        except Exception:
            logger.exception("backfill_missing_llm_titles failed")
