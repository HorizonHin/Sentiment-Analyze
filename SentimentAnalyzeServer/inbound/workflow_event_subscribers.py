from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from SentimentAnalyzeServer.application.common import (
    EVENT_CRAWL_SAVED,
    EVENT_SENTIMENT_ANALYZED,
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
        self.event_manager = EventManager()
        self.sentiment_app_service = sentiment_app_service
        self.topic_app_service = topic_app_service
        self.crawl_interval_seconds = max(60, int(crawl_interval_seconds))

    def register(self) -> None:
        self.event_manager.subscribe(EVENT_CRAWL_SAVED, self._on_crawl_saved)
        self.event_manager.subscribe(EVENT_SENTIMENT_ANALYZED, self._on_sentiment_analyzed)

    def _on_crawl_saved(self, payload: Dict[str, Any]) -> None:
        saved_items = payload.get("saved_items", [])
        if not isinstance(saved_items, list) or not saved_items:
            return

        valid_items: List[NewsItem] = [item for item in saved_items if isinstance(item, NewsItem)]
        if not valid_items:
            return

        try:
            self.sentiment_app_service.analyze_and_update_news_items(valid_items)
        except Exception:
            logger.exception("analyze_and_update_news_items failed")

    def _on_sentiment_analyzed(self, payload: Dict[str, Any]) -> None:
        # 在情感分析完成后，尝试推荐和缓存一个动态窗口内的热点话题
        lookback_seconds = self.crawl_interval_seconds * _TOPIC_LOOKBACK_MULTIPLIER
        start_time = datetime.now() - timedelta(seconds=lookback_seconds)
        end_time = datetime.now()

        try:
            self.topic_app_service.recommend_and_cache_topics(
                start_time=start_time,
                end_time=end_time,
            )
        except Exception:
            logger.exception("recommend_and_cache_topics failed")
