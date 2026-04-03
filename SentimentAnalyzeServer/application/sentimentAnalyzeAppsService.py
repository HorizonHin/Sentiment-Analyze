from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from SentimentAnalyzeServer.system.infra import (
	EVENT_SENTIMENT_ANALYZED,
	REDIS_KEY_LATEST_NOT_NEED_ANALYSIS_NEWS,
	REDIS_KEY_RECENT_30M_ANALYZED_NEWS,
	CommonThreadPool,
	EventManager,
	MyRedis,
)
from SentimentAnalyzeServer.application.common import is_item_analysis_pending, is_source_support_comments
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
		news_domain_service: Optional[NewsDomainService] = None,
	) -> None:
		self.news_domain_service = news_domain_service or NewsDomainService(storage)
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

	def _cache_latest_not_need_analysis_items(self, items: List[NewsItem]) -> None:
		payload = self._serialize_news_items(items)
		self.redis.set(REDIS_KEY_LATEST_NOT_NEED_ANALYSIS_NEWS, payload)

	def _generate_latest_rank_board(self, items: List[NewsItem]) -> List[NewsItem]:
		# 使用 (source_id, latest_rank) 作为唯一键，确保每个平台每个排名只有一个条目
		# 如果没有排名（latest_rank 为 None），则使用 title 作为降级唯一键
		# 对于重复项，保留时间戳（last_time 或 analyzed_time）更新的
		identity_to_item: Dict[tuple[str, Any], NewsItem] = {}
		
		for item in items:
			source_id = str(item.source_id or "").strip()
			if not source_id:
				continue
				
			rank = item.latest_rank
			if rank is not None:
				# 针对有排名的，以排名为准
				key = (source_id, f"rank_{rank}")
			else:
				# 针对无排名的（比如某些增量新闻），以标题作为降级 key
				title = str(item.title or "").strip()
				if not title:
					continue
				key = (source_id, f"title_{title}")

			existing = identity_to_item.get(key)
			if existing is None:
				identity_to_item[key] = item
				continue

			def _get_ts(ni: NewsItem) -> int:
				ts = int(ni.last_time or 0)
				# 如果有分析时间，优先使用分析时间作为新鲜度判断依据
				if ni.analyzed_time:
					ts = max(ts, int(ni.analyzed_time.timestamp()))
				return ts

			# 如果新条目的时间戳更新，或者时间戳相同但内容更完整（已有 summary），则替换
			if _get_ts(item) > _get_ts(existing):
				identity_to_item[key] = item
			elif _get_ts(item) == _get_ts(existing):
				# 如果时间也一致，优先保留有 summary 的（防止被刚爬下来还没分析的覆盖）
				if (item.summary or item.analyzed_time) and not (existing.summary or existing.analyzed_time):
					identity_to_item[key] = item

		return list(identity_to_item.values())

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

	def analyze_and_update_news_items(self, items: List[NewsItem], should_cache: bool = True) -> bool:
		""""一旦执行这个方法，就一定会发出分析完成的事件（即使没有任何数据被分析）。"""
		# 过滤规则：使用公共方法判断是否需要分析
		pending_items: List[NewsItem] = []
		not_need_analysis_items: List[NewsItem] = []
		for item in items:
			if is_item_analysis_pending(item):
				pending_items.append(item)
			else:
				not_need_analysis_items.append(item)
		
		updated_items: List[NewsItem] = []
		if not pending_items:
			logger.info("No pending news items to analyze. input_count=%s", len(items))
		else:
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

		# 仅在 should_cache 为 True 时更新 Redis 热门缓存（通常来自实时爬取）
		if should_cache:
			now_ts = int(time.time())
			recent_threshold = now_ts - self.recent_window_seconds
			
			# 将本次 crawl 和处理的所有项合并
			final_items = updated_items + not_need_analysis_items
			
			recent_items = [
				item for item in final_items
				if item.last_time is not None and item.last_time >= recent_threshold
			]
			if recent_items:
				logger.debug("Caching %d recent items (< %d sec)", len(recent_items), self.recent_window_seconds)
				self.common_thread_pool.submit(self._cache_recent_analyzed_news, recent_items)

			# 存储最新“当前已分析”项，无论是否支持重分析
			# 只要 analyzed_time 不为空（有过 LLM 返回），就属于最新分析看板的后备
			latest_analyzed = [
				item for item in final_items
				if item.analyzed_time or item.summary
			]
			if latest_analyzed:
				logger.debug("Caching %d items as latest-analyzed results", len(latest_analyzed))
				self.common_thread_pool.submit(self._cache_latest_not_need_analysis_items, latest_analyzed)

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
		# 检查是否支持评论抓取，且评论字段确实有内容
		supports_comments = is_source_support_comments(item.source_id or "")
		has_comments = bool(item.comments)

		for attempt in range(1, self.max_retries + 1):
			try:
				# 只有当平台支持评论且抓取到了评论时，才调用带评论的分析方法
				if supports_comments and has_comments:
					result = self.llm_domain_analyzer.analyze_title_and_comments(item.title, comments=item.comments)
				else:
					# 否则（不支持评论，或支持但没抓到评论），降级为仅分析标题
					result = self.llm_domain_analyzer.analyze_title_only(item.title)
					
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
		if not items:
			return []
		return [
			item
			for item in items
			if not (item.analyzed_time)
		]
  
	def analyze_pending_items_by_latest_time(
		self,
	) -> dict[str, Any]:
		"""获取最新一批未分析的新闻并执行分析。"""
		resolved_first_time = int(time.time()) - self.first_time_lookback_seconds
		latest_items = self.news_domain_service.get_news_list_by_latest_batch(
			isAnalyzed=False,
			first_time=resolved_first_time,
		)
		if not latest_items:
			logger.info("[Analyze_pending] 无可分析的数据")
			return {"success": True, "item_count": 0}
		
		pending_items = self.filter_news_items_not_analyzed(latest_items)
		if not pending_items:
			logger.info("[Analyze_pending] 无可分析的数据")
			return {"success": True, "item_count": 0}

		# 对于补救措施分析任务，不应更新热门数据的 Redis 缓存
		saved = self.analyze_and_update_news_items(pending_items, should_cache=False)
		analyzed_count = len(pending_items) if saved else 0
		return {"success": True, "item_count": analyzed_count}

	def get_latest_analyzed_news_batch_grouped(self) -> Dict[str, List[NewsItem]]:
		"""获取最新一批已分析的新闻，按 source_id 分组。
		
		返回流程：
		1. 优先尝试 Redis 缓存（30分钟内的最新分析数据 + 已无需分析的数据）
		2. 缓存为空或被过滤后，查询数据库获取 TOP 500 最新已分析新闻
		3. Domain 层进一步过滤确保数据完整性
		"""
		resolved_first_time = int(time.time()) - self.first_time_lookback_seconds
		
		# 尝试从 Redis 缓存读取最新数据
		cached_payload_map = self.redis.get_many(
			[
				REDIS_KEY_LATEST_NOT_NEED_ANALYSIS_NEWS,
				REDIS_KEY_RECENT_30M_ANALYZED_NEWS,
			]
		)
		if cached_payload_map:
			cached_items: List[NewsItem] = []
			for payload in cached_payload_map.values():
				cached_items.extend(self._deserialize_news_items(payload))

			if cached_items:
				initial_count = len(cached_items)
				cached_items = self._generate_latest_rank_board(cached_items)
				dedup_count = len(cached_items)
				
				# 过滤及排序：保留所有至少有初步分析结果的项（含 pending 重分析的）
				# 如果某平台在缓存中完全没有已分析数据，则保留所有。
				# is_item_analysis_pending 为 True 表示“建议重分析”，但通常已有 summary
				cached_items = [
					item
					for item in cached_items
					if item.analyzed_time or item.summary  # 只要有过分析结果就返回
				]
				after_filter = len(cached_items)
				
				logger.debug(
					"Redis cache: initial=%d items, dedup=%d items, after analysis check=%d items",
					initial_count, dedup_count, after_filter
				)
				
				if cached_items:
					logger.info("Returning %d latest analyzed items from Redis cache", len(cached_items))
					return self.news_domain_service.group_news_items_by_platform(cached_items)
				else:
					logger.warning("Redis cache exists but filtered to empty; falling back to DB query")

		# 后备：查询数据库获取最新已分析的新闻
		logger.info("Querying database for latest analyzed news")
		grouped = self.news_domain_service.get_latest_analyzed_news_batch_grouped_by_source(
			first_time=resolved_first_time
		)
		if not grouped:
			return {}

		# 再次去重，确保每个平台 (source_id + title) 唯一且是最新 (last_time DESC)
		for source_id, items in grouped.items():
			grouped[source_id] = self._generate_latest_rank_board(items)

		return grouped
	
	