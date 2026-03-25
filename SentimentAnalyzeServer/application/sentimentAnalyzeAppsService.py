from __future__ import annotations

from datetime import datetime
import logging
import time
from typing import Any, Dict, List, Optional

from SentimentAnalyzeServer.application.common import EVENT_SENTIMENT_ANALYZED, EventManager
from SentimentAnalyzeServer.application.tools.llmExecutorService import LLMExecutorService
from SentimentAnalyzeServer.domain.llmAnalyzer.llmAnalyzer import LLMTitleAnalyzer
from SentimentAnalyzeServer.domain.news.news import Entity, Keyword, NewsItem, NewsDomainService


logger = logging.getLogger(__name__)


class SentimentAnalyzeAppService:
	def __init__(self, storage: object, analyzer: LLMTitleAnalyzer, max_workers: int = 32) -> None:
		self.news_domain_service = NewsDomainService(storage)
		self.llm_domain_analyzer = analyzer
		self.event_manager = EventManager()
		self.max_retries = 5
		self.retry_delay_seconds = 1
		self.batch_save_size = 20
		self.executor_service = LLMExecutorService(max_workers=max_workers)

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

	def analyze_pending_items_by_first_time(
		self,
		start_time: Optional[datetime] = None,
		end_time: Optional[datetime] = None,
	) -> bool:
		all_items = self.news_domain_service.get_news_list_by_firt_time_range(
			isAnalyzed=False,
			start_time=start_time,
			end_time=end_time,
		)
		if all_items is None:
			logger.info("[ScheduledCrawler] 无可分析的数据")
			return None
		pending_items = self.filter_news_items_not_analyzed(all_items)
		if not pending_items:
			logger.info("[ScheduledCrawler] 无可分析的数据")
			return None
		saved = self.analyze_and_update_news_items(pending_items)
		return saved

	def analyze_pending_items_by_latest_time(
		self,
		start_time: Optional[datetime] = None,
		end_time: Optional[datetime] = None,
	) -> dict[str, Any]:
		latest_items = self.news_domain_service.get_news_list_by_latest_crawl_range(
			isAnalyzed=False,
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
		start_time: Optional[datetime] = None,
		end_time: Optional[datetime] = None,
	) -> Dict[str, List[NewsItem]]:
		"""按 last_time 范围获取已分析新闻，并按 source_id 分组。"""
		grouped = self.news_domain_service.get_group_news_by_latest_crawl_range(
			isAnalyzed=True,
			start_time=start_time,
			end_time=end_time,
		)
		return grouped or {}
	
	