from __future__ import annotations

import logging
import time
from typing import Any, Dict, List

from SentimentAnalyzeServer.system.infra import (
    EVENT_CRAWL_SAVED,
    EVENT_SENTIMENT_ANALYZED,
    EVENT_TOPIC_RANK_UPDATED,
    EVENT_TOPIC_TITLE_SUMMARY_BLOCKED,
    EventManager,
)
from SentimentAnalyzeServer.application.sentimentAnalyzeAppsService import SentimentAnalyzeAppService
from SentimentAnalyzeServer.application.topicAppService import TopicAppService
from SentimentAnalyzeServer.domain.news.news import NewsItem
from SentimentAnalyzeServer.domain.risk.risk import RiskWarningDomainService
from SentimentAnalyzeServer.domain.topic.topic import Topic


logger = logging.getLogger(__name__)


_TOPIC_LOOKBACK_MULTIPLIER = 12.4


class WorkflowEventSubscribers:
    def __init__(
        self,
        sentiment_app_service: SentimentAnalyzeAppService,
        topic_app_service: TopicAppService,
        risk_warning_domain_service: RiskWarningDomainService,
        crawl_interval_seconds: int,
    ) -> None:
        self._is_registered = False
        self.event_manager = EventManager()
        self.sentiment_app_service = sentiment_app_service
        self.topic_app_service = topic_app_service
        self.risk_warning_domain_service = risk_warning_domain_service
        self.crawl_interval_seconds = max(60, int(crawl_interval_seconds))

    def register(self) -> None:
        if self._is_registered:
            logger.warning("WorkflowEventSubscribers already registered, skipping.")
            return
        self.event_manager.subscribe(EVENT_CRAWL_SAVED, self._on_crawl_saved)
        self.event_manager.subscribe(EVENT_SENTIMENT_ANALYZED, self._on_sentiment_analyzed)
        self.event_manager.subscribe(EVENT_TOPIC_RANK_UPDATED, self._on_topic_rank_updated)
        self.event_manager.subscribe(EVENT_TOPIC_RANK_UPDATED, self._on_topic_risk_warning_detected)
        self.event_manager.subscribe(EVENT_TOPIC_TITLE_SUMMARY_BLOCKED, self._on_topic_title_summary_blocked)
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
        logger.info("处理Topic排名更新事件，触发LLM分析llm_title。raw_limit=%s", raw_limit)
        try:
            limit = max(1, min(200, int(raw_limit)))
        except (TypeError, ValueError):
            limit = 50

        try:
            self.topic_app_service.backfill_missing_llm_titles(limit=limit)
        except Exception:
            logger.exception("backfill_missing_llm_titles failed")

    def _on_topic_risk_warning_detected(self, payload: Dict[str, Any]) -> None:
        try:
            topics_from_event = payload.get("topics", [])
            event_topics: List[Topic] = [item for item in topics_from_event if isinstance(item, Topic)]
            occurred_at = int(payload.get("end_time") or int(time.time()))
            inserted_count = self.risk_warning_domain_service.evaluate_and_record_topic_risks(
                topics=event_topics,
                occurred_at=occurred_at,
                detected_by_event=EVENT_TOPIC_RANK_UPDATED,
            )
            logger.info(
                "话题风险评估完成。topic_count=%s, inserted_count=%s",
                len(event_topics),
                inserted_count,
            )
        except Exception:
            logger.exception("evaluate_and_record_topic_risks failed")

    def _on_topic_title_summary_blocked(self, payload: Dict[str, Any]) -> None:
        enriched_payload = dict(payload or {})
        topic_obj = enriched_payload.get("topic")
        if isinstance(topic_obj, Topic):
            enriched_payload["topic_created_at"] = int(getattr(topic_obj, "created_at", 0) or 0)
            enriched_payload["topic_id"] = int(getattr(topic_obj, "id", -1) or -1)
            if not str(enriched_payload.get("topic_name", "") or "").strip():
                enriched_payload["topic_name"] = str(getattr(topic_obj, "topic", "") or "")

        logger.info(
            "处理话题标题总结审核拦截事件。topic_created_at=%s, topic_id=%s",
            enriched_payload.get("topic_created_at"),
            enriched_payload.get("topic_id"),
        )
        try:
            ok = self.risk_warning_domain_service.record_sensitive_title_block(enriched_payload)
            if not ok:
                logger.warning(
                    "record_sensitive_title_block skipped. topic_created_at=%s, topic_id=%s",
                    enriched_payload.get("topic_created_at"),
                    enriched_payload.get("topic_id"),
                )
        except Exception:
            logger.exception("record_sensitive_title_block failed")
