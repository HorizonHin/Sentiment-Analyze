from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from SentimentAnalyzeServer.system.infra import (
	EVENT_SENTIMENT_ANALYZED,
	REDIS_KEY_LATEST_UPDATED_ANALYZED_NEWS,
	REDIS_KEY_RECENT_30M_ANALYZED_NEWS,
	CommonThreadPool,
	EventManager,
	MyRedis,
)
from SentimentAnalyzeServer.domain.llmAnalyzer.llmExecutorService import LLMExecutorService
from SentimentAnalyzeServer.domain.llmAnalyzer.llmAnalyzer import LLMTitleAnalyzer
from SentimentAnalyzeServer.domain.news.news import Entity, Keyword, NewsItem, NewsDomainService


logger = logging.getLogger(__name__)


class SentimentAnalyzeAppService:
	def __init__(
		self,
		storage: object,
		analyzer: LLMTitleAnalyzer,
		max_workers: int = 32,
		recent_window_seconds: int = 30 * 60,
		first_time_lookback_seconds: int = 7 * 24 * 60 * 60,
	) -> None:
		self.news_domain_service = NewsDomainService(storage)
		self.llm_domain_analyzer = analyzer
		self.event_manager = EventManager()
		self.redis = MyRedis()
		self.common_thread_pool = CommonThreadPool()
		self.recent_window_seconds = max(60, int(recent_window_seconds))
		self.first_time_lookback_seconds = max(60, int(first_time_lookback_seconds))
		self.max_retries = 5
		self.retry_delay_seconds = 1
		self.batch_save_size = 20
		self.executor_service = LLMExecutorService(max_workers=max_workers)

	def _serialize_news_items(self, items: List[NewsItem]) -> List[Dict[str, Any]]:
		return [item.to_dict() for item in items]

	def _deserialize_news_items(self, payload: Any) -> List[NewsItem]:
		if not isinstance(payload, list):
			return []

		result: List[NewsItem] = []
		for raw in payload:
			if not isinstance(raw, dict):
				continue
			try:
				result.append(NewsItem.from_dict(raw))
			except Exception:
				logger.exception("Failed to deserialize cached news item.")
		return result

	def _cache_recent_analyzed_news(self, items: List[NewsItem]) -> None:
		payload = self._serialize_news_items(items)
		self.redis.set(REDIS_KEY_RECENT_30M_ANALYZED_NEWS, payload)

	def _deduplicate_news_items(self, items: List[NewsItem]) -> List[NewsItem]:
		def _score(news_item: NewsItem) -> int:
			if news_item.analyzed_time is not None:
				return int(news_item.analyzed_time.timestamp())
			return int(news_item.last_time or 0)

		key_to_item: Dict[tuple[str, str], NewsItem] = {}
		for item in items:
			key = (item.source_id or "", item.title or "")
			if key == ("", ""):
				continue
			existing = key_to_item.get(key)
			if existing is None:
				key_to_item[key] = item
				continue

			existing_ts = _score(existing)
			incoming_ts = _score(item)
			if incoming_ts >= existing_ts:
				key_to_item[key] = item

		return list(key_to_item.values())

	def _filter_items_by_last_time_range(
		self,
		items: List[NewsItem],
		start_time: Optional[int],
		end_time: Optional[int],
	) -> List[NewsItem]:
		if start_time is None and end_time is None:
			return items

		filtered: List[NewsItem] = []
		for item in items:
			if item.last_time is None:
				continue
			if start_time is not None and end_time is not None:
				if start_time <= end_time:
					if start_time <= item.last_time <= end_time:
						filtered.append(item)
				else:
					if item.last_time >= start_time or item.last_time <= end_time:
						filtered.append(item)
			elif start_time is not None:
				if item.last_time >= start_time:
					filtered.append(item)
			elif end_time is not None:
				if item.last_time <= end_time:
					filtered.append(item)

		return filtered

	def analyze_and_update_news_items(self, items: List[NewsItem]) -> bool:
		if not items:
			logger.debug("No input items for analysis.")
			return False

		pending_items = self.filter_news_items_not_analyzed(items)
		if not pending_items:
			logger.info("No pending news items to analyze. input_count=%s", len(items))
			return False

		updated_items: List[NewsItem] = []

		def consume_batch(batch: List[Any]) -> None:
			news_batch = [item for item in batch if isinstance(item, NewsItem)]
			if not news_batch:
				return
			if self._save_batch_with_retry(news_batch):
				updated_items.extend(news_batch)

		batch_manager = self.executor_service.create_queue_batch_manager(
			batch_size=self.batch_save_size,
			consume_batch=consume_batch,
		)

		futures = [
			self.executor_service.execute(self._analyze_with_retry, item)
			for item in pending_items
		]

		try:
			for future in futures:
				try:
					result_item = future.result()
				except Exception:
					logger.exception("Unexpected error in LLM executor future.")
					continue

				if result_item is not None:
					batch_manager.put(result_item)
		finally:
			batch_manager.close_and_wait()

		if not updated_items:
			logger.warning(
				"Analysis finished but no items were persisted. pending_count=%s",
				len(pending_items),
			)
			return False

		logger.info(
			"Analysis and persistence completed. pending_count=%s, persisted_count=%s",
			len(pending_items),
			len(updated_items),
		)
		now_ts = int(time.time())
		recent_threshold = now_ts - self.recent_window_seconds
		recent_items = [
			item for item in updated_items
			if item.last_time is not None and item.last_time >= recent_threshold
		]
		if recent_items:
			self.common_thread_pool.submit(self._cache_recent_analyzed_news, recent_items)

		self.event_manager.publish(
			EVENT_SENTIMENT_ANALYZED,
			{
				"analyzed_items": updated_items,
				"pending_count": len(pending_items),
				"persisted_count": len(updated_items),
			},
		)
		logger.info("Sentiment analysis flow completed successfully. persisted_count=%s", len(updated_items))
		return True

	def _analyze_with_retry(self, item: NewsItem) -> Optional[NewsItem]:
		for attempt in range(1, self.max_retries + 1):
			try:
				result = self.llm_domain_analyzer.analyze_title(item.title)
				self.apply_llm_result(item, result)
				return item
			except Exception:
				logger.exception(
					"Analyze title failed. title=%s, attempt=%s/%s",
					item.title,
					attempt,
					self.max_retries,
				)
				if attempt < self.max_retries:
					time.sleep(self.retry_delay_seconds)

		logger.error("Analyze title failed after retries; skipping item. title=%s", item.title)
		return None

	def _save_batch_with_retry(self, batch: List[NewsItem]) -> bool:
		for attempt in range(1, self.max_retries + 1):
			try:
				saved = self.news_domain_service.update_news_list(batch)
				if not saved:
					raise RuntimeError("update_news_list returned False")
				return True
			except Exception:
				logger.exception(
					"Batch persistence failed. attempt=%s/%s, batch_size=%s",
					attempt,
					self.max_retries,
					len(batch),
				)
				if attempt < self.max_retries:
					time.sleep(self.retry_delay_seconds)

		logger.error("Batch persistence failed after retries; dropping batch. batch_size=%s", len(batch))
		return False

	def apply_llm_result(self, item: NewsItem, result: Dict) -> None:
		item.event_type = str(result.get("event_type", ""))
		item.summary = str(result.get("summary", ""))

		entities = result.get("entities", [])
		if isinstance(entities, list):
			item.entities = [
				Entity(name=str(entity.get("name", "")), type=str(entity.get("type", "")))
				for entity in entities
				if isinstance(entity, dict)
			]

		keywords = result.get("keywords", [])
		if isinstance(keywords, list):
			item.keywords = [
				Keyword(term=str(keyword.get("term", "")), importance=float(keyword.get("importance", 0.0)))
				for keyword in keywords
				if isinstance(keyword, dict)
			]

		sentiment = result.get("sentiment_analysis", {})
		if isinstance(sentiment, dict):
			item.sentiment_polarity = str(sentiment.get("polarity", ""))
			item.positive_ratio = float(sentiment.get("positive_ratio", None))
			item.negative_ratio = float(sentiment.get("negative_ratio", None))
			item.neutral_ratio = float(sentiment.get("neutral_ratio", None))

			dimensions = sentiment.get("dimensions", {})
			if isinstance(dimensions, dict):
				item.optimism_score = float(dimensions.get("optimism", 0.0))
				item.trust_score = float(dimensions.get("trust", 0.0))
				item.attention_score = float(dimensions.get("attention", 0.0))
				item.controversy_score = float(dimensions.get("controversy", 0.0))

		item.analyzed_time = datetime.now()
		item.deduplicate_entities_and_keywords()

	def filter_news_items_not_analyzed(self, items: List[NewsItem]) -> List[NewsItem]:
		return [
			item
			for item in items
			if not (item.analyzed_time or item.sentiment_polarity or item.entities or item.keywords)
		]

	def analyze_pending_items_by_latest_time(
		self,
		first_time: Optional[int] = None,
		start_time: Optional[int] = None,
		end_time: Optional[int] = None,
	) -> dict[str, Any]:
		resolved_first_time = int(first_time) if first_time is not None else int(time.time()) - self.first_time_lookback_seconds
		latest_items = self.news_domain_service.get_news_list_by_latest_crawl_range(
			isAnalyzed=False,
			first_time=resolved_first_time,
			start_time=start_time,
			end_time=end_time,
		)
		if not latest_items:
			logger.info("[ScheduledCrawler] 无可分析的数据")
			return {"success": False, "reason": "no_data"}
		pending_items = self.filter_news_items_not_analyzed(latest_items)
		if not pending_items:
			logger.info("[ScheduledCrawler] 无可分析的数据")
			return {"success": True, "item_count": 0}

		saved = self.analyze_and_update_news_items(pending_items)
		analyzed_count = len(pending_items) if saved else 0
		return {"success": True, "item_count": analyzed_count}

	def get_analyzed_news_grouped_by_latest_time(
		self,
		first_time: Optional[int] = None,
		start_time: Optional[int] = None,
		end_time: Optional[int] = None,
	) -> Dict[str, List[NewsItem]]:
		"""按 last_time 范围获取已分析新闻，并按 source_id 分组。"""
		resolved_first_time = int(first_time) if first_time is not None else int(time.time()) - self.first_time_lookback_seconds
		cached_payload_map = self.redis.get_many(
			[
				REDIS_KEY_LATEST_UPDATED_ANALYZED_NEWS,
				REDIS_KEY_RECENT_30M_ANALYZED_NEWS,
			]
		)
		if cached_payload_map:
			cached_items: List[NewsItem] = []
			for payload in cached_payload_map.values():
				cached_items.extend(self._deserialize_news_items(payload))

			if cached_items:
				cached_items = self._deduplicate_news_items(cached_items)
				cached_items = [
					item
					for item in cached_items
					if item.first_time is not None and item.first_time >= resolved_first_time
				]
				cached_items = self._filter_items_by_last_time_range(cached_items, start_time, end_time)
				if cached_items:
					return self.news_domain_service.group_news_items_by_platform(cached_items)

		grouped = self.news_domain_service.get_group_news_by_latest_crawl_range(
			isAnalyzed=True,
			first_time=resolved_first_time,
			start_time=start_time,
			end_time=end_time,
		)
		return grouped or {}
	
	