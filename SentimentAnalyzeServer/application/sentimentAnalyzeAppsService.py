from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from SentimentAnalyzeServer.system.infra import (
	EVENT_SENTIMENT_ANALYZED,
	REDIS_KEY_LATEST_NOT_NEED_ANALYSIS_NEWS,
	REDIS_KEY_RECENT_30M_ANALYZED_NEWS,
	CommonThreadPool,
	QueueBatchManager,
	EventManager,
	MyRedis,
)
from SentimentAnalyzeServer.application.common import is_item_analysis_pending, is_source_support_comments
from SentimentAnalyzeServer.domain.llmAnalyzer.llmAnalyzer import LLMAnalyzer, LLMTitleAnalyzer
from SentimentAnalyzeServer.domain.news.news import Entity, NewsKeyword, NewsItem, NewsDomainService


logger = logging.getLogger(__name__)


class SentimentAnalyzeAppService:
	def __init__(
		self,
		storage: object,
		analyzer: LLMAnalyzer,
		max_workers: int = 32,
		recent_window_seconds: int = 30 * 60,
		first_time_lookback_seconds: int = 7 * 24 * 60 * 60,
		news_domain_service: Optional[NewsDomainService] = None,
		max_analysis_workers: Optional[int] = 5,
	) -> None:
		self.news_domain_service = news_domain_service or NewsDomainService(storage)
		self.llm_domain_analyzer = analyzer
		self.event_manager = EventManager()
		self.redis = MyRedis()
		self.common_thread_pool = CommonThreadPool()
		self.recent_window_seconds = max(60, int(recent_window_seconds))
		self.first_time_lookback_seconds = max(60, int(first_time_lookback_seconds))
		self.batch_save_size = 20
		self.max_analysis_workers = max_analysis_workers
		
		# 初始化生产者消费者模型（用于落库）
		self._db_batch_manager: Optional[QueueBatchManager] = None

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
		self.redis.set(REDIS_KEY_RECENT_30M_ANALYZED_NEWS, payload, ttl_seconds=1800)

	def _cache_latest_not_need_analysis_items(self, items: List[NewsItem]) -> None:
		payload = self._serialize_news_items(items)
		self.redis.set(REDIS_KEY_LATEST_NOT_NEED_ANALYSIS_NEWS, payload, ttl_seconds=3600)

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
		try:
			# 1. 过滤需要分析的项
			pending_items = [item for item in items if is_item_analysis_pending(item)]
			not_need_analysis_items = [item for item in items if not is_item_analysis_pending(item)]
			
			updated_items: List[NewsItem] = []
			
			if not pending_items:
				logger.info("No pending news items to analyze. input_count=%s", len(items))
			else:
				# 2. 定义异步分析流程
				async def run_analysis_flow():
					sem = asyncio.Semaphore(self.max_analysis_workers)
					updated_lock = threading.Lock()
					
					def consume_db_batch(batch: List[NewsItem]):
						if self._save_batch_with_retry(batch):
							with updated_lock:
								updated_items.extend(batch)

					batch_manager = self.common_thread_pool.create_queue_batch_manager(
						batch_size=self.batch_save_size,
						consume_batch=consume_db_batch
					)
					
					async def analyze_task(item: NewsItem):
						async with sem:
							if await self._analyze_with_retry(item):
								batch_manager.put(item)

					try:
						await asyncio.gather(*(analyze_task(item) for item in pending_items))
					finally:
						batch_manager.close_and_wait()

				# 3. 执行异步桥接
				try:
					loop = asyncio.get_event_loop()
					if loop.is_running():
						import nest_asyncio
						nest_asyncio.apply()
						loop.run_until_complete(run_analysis_flow())
					else:
						loop.run_until_complete(run_analysis_flow())
				except Exception:
					logger.exception("Async analysis execution failed")

			# 4. 更新缓存
			if should_cache:
				self._update_redis_caches(updated_items, not_need_analysis_items)

			# 5. 发布事件 (确保 payload 序列化且使用 .to_dict())
			self.event_manager.publish(EVENT_SENTIMENT_ANALYZED, {
				"analyzed_items": [t.to_dict() for t in updated_items],
				"pending_count": len(pending_items),
				"persisted_count": len(updated_items),
			})
			
			logger.info("Sentiment analysis flow completed. persisted_count=%s", len(updated_items))
			return True
		except Exception:
			logger.exception("Error in analyze_and_update_news_items")
			return False

	def _update_redis_caches(self, updated: List[NewsItem], others: List[NewsItem]) -> None:
		now_ts = int(time.time())
		recent_threshold = now_ts - self.recent_window_seconds
		final_items = updated + others
		
		recent = [i for i in final_items if i.last_time and i.last_time >= recent_threshold]
		if recent:
			self.common_thread_pool.submit(self._cache_recent_analyzed_news, recent)

		latest = [i for i in final_items if i.analyzed_time or i.summary]
		if latest:
			self.common_thread_pool.submit(self._cache_latest_not_need_analysis_items, latest)

	async def _analyze_with_retry(self, item: NewsItem) -> Optional[NewsItem]:
		# 检查是否支持评论抓取，且评论字段确实有内容
		supports_comments = is_source_support_comments(item.source_id or "")
		has_comments = bool(item.comments)

		try:
			# 限流和重试逻辑已下沉到 llm_domain_analyzer 内部
			if supports_comments and has_comments:
				result = await self.llm_domain_analyzer.analyze_title_and_comments(item.title, comments=item.comments)
			else:
				result = await self.llm_domain_analyzer.analyze_title_only(item.title)
				
			self._apply_llm_result(item, result)
			return item
		except Exception:
			logger.exception("Final LLM analysis failure for item: %s", item.title)
			return None

	def _save_batch_with_retry(self, batch: List[NewsItem]) -> bool:
		# 数据库保存依然保留简单的重试逻辑，因为 domain 层不处理数据库重试
		max_retries = 3
		retry_delay = 1
		for attempt in range(1, max_retries + 1):
			try:
				saved = self.news_domain_service.update_news_list(batch)
				if not saved:
					raise RuntimeError("update_news_list returned False")
				return True
			except Exception:
				logger.exception(
					"Batch persistence failed. attempt=%s/%s, batch_size=%s",
					attempt,
					max_retries,
					len(batch),
				)
				if attempt < max_retries:
					time.sleep(retry_delay)

		logger.error("Batch persistence failed after retries; dropping batch. batch_size=%s", len(batch))
		return False

	def _apply_llm_result(self, item: NewsItem, result: Dict) -> None:
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
				NewsKeyword(term=str(keyword.get("term", "")), importance=float(keyword.get("importance", 0.0)))
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

	def _filter_news_items_not_analyzed(self, items: List[NewsItem]) -> List[NewsItem]:
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
		try:
			latest_items = self.news_domain_service.get_news_list_by_latest_batch(
				isAnalyzed=False,
			)
			if not latest_items:
				logger.info("[Analyze_pending] 无可分析的数据")
				return {"success": True, "item_count": 0}
			
			pending_items = self._filter_news_items_not_analyzed(latest_items)
			if not pending_items:
				logger.info("[Analyze_pending] 无可分析的数据")
				return {"success": True, "item_count": 0}

			# 对于补救措施分析任务，不应更新热门数据的 Redis 缓存
			saved = self.analyze_and_update_news_items(pending_items, should_cache=False)
			analyzed_count = len(pending_items) if saved else 0
			return {"success": True, "item_count": analyzed_count}
		except Exception:
			logger.exception("Error in analyze_pending_items_by_latest_time")
			return {"success": False, "item_count": 0, "error": "Internal error"}

	def get_latest_analyzed_news_batch_grouped(self) -> Dict[str, List[NewsItem]]:
		"""获取最新一批已分析的新闻，按 source_id 分组。"""
		try:
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
			grouped = self.news_domain_service.get_latest_analyzed_news_batch_grouped_by_source()
			if not grouped:
				return {}

			# 再次去重，确保每个平台 (source_id + title) 唯一且是最新 (last_time DESC)
			for source_id, items in grouped.items():
				grouped[source_id] = self._generate_latest_rank_board(items)

			return grouped
		except Exception:
			logger.exception("Error in get_latest_analyzed_news_batch_grouped")
			return {}

	