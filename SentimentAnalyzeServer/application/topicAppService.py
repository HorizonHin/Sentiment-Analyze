from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
from collections import defaultdict
from concurrent.futures import as_completed
import logging
import time
from typing import Any, Dict, List, Optional

from SentimentAnalyzeServer.system.infra import CommonThreadPool, singleton_task
from SentimentAnalyzeServer.system.infra import EventManager, EVENT_TOPIC_RANK_UPDATED
from SentimentAnalyzeServer.application.common import Result
from SentimentAnalyzeServer.domain.llmAnalyzer.llmAnalyzer import LLMAnalyzer, LLMTitleAnalyzer
from SentimentAnalyzeServer.domain.news.news import Entity, NewsKeyword, NewsDomainService, NewsItem
from SentimentAnalyzeServer.domain.topic.topic import Topic, TopicDomainService

logger = logging.getLogger(__name__)

_TOPIC_LOOKBACK_MULTIPLIER = 12.2


class TopicCacheManager(ABC):
    # 热门话题数量上限暂时定为60个


    @abstractmethod
    def save_or_update_topics_cache(self, topics: List[Topic], limit: int = 60) -> None:
        """保存或更新话题列表，根据"""
        pass

    @abstractmethod
    def get_topics(self) -> List[Topic]:
        pass

    @abstractmethod
    def get_topic_by_composite_key(self, topic_created_at: int, topic_id: int) -> Optional[Topic]:
        pass

    @abstractmethod
    def get_topic_by_composite_keys(self, keys: List[tuple[int, int]]) -> List[Topic]:
        pass

class TopicCacheManager_Memory(TopicCacheManager):
    def __init__(self) -> None:
        self._topics: List[Topic] = []

    @staticmethod
    def _topic_key(topic: Topic) -> str:
        name = (topic.topic or "").strip()
        if name:
            return name
        if int(getattr(topic, "id", -1) or -1) > 0:
            return f"id:{int(topic.id)}"
        return f"obj:{id(topic)}"


    @classmethod
    def _eviction_score(cls, topic: Topic, now_ts: int) -> float:
        updated_at = topic.updated_at
        age_seconds = max(0, now_ts - int(updated_at or 0)) if updated_at else 10**9
        age_hours = age_seconds / 3600.0
        freshness = 1.0 / (1.0 + age_hours)
        return topic.total_weight * freshness

    def save_or_update_topics_cache(self, topics: List[Topic], limit: int = 60) -> None:
        capped_limit = max(1, limit)
        now_ts = int(time.time())

        merged: Dict[str, Topic] = {}
        for topic in self._topics + list(topics):
            key = self._topic_key(topic)
            existing = merged.get(key)
            if existing is None:
                merged[key] = topic
                continue

            current_updated = existing.updated_at
            incoming_updated = topic.updated_at
            if incoming_updated > current_updated:
                merged[key] = topic
                continue
            if incoming_updated == current_updated and topic.total_weight > existing.total_weight:
                merged[key] = topic

        sorted_topics = sorted(
            merged.values(),
            key=lambda t: (
                self._eviction_score(t, now_ts),
                t.updated_at,
                t.total_weight,
            ),
            reverse=True,
        )
        self._topics = sorted_topics[:capped_limit]

    def get_topics(self) -> List[Topic]:
        return list(self._topics)

    def get_topic_by_composite_key(self, topic_created_at: int, topic_id: int) -> Optional[Topic]:
        return self.get_topic_by_composite_keys([(topic_created_at, topic_id)])[0] if self.get_topic_by_composite_keys([(topic_created_at, topic_id)]) else None

    def get_topic_by_composite_keys(self, keys: List[tuple[int, int]]) -> List[Topic]:
        results = []
        key_set = set(keys)
        for topic in self._topics:
            if (int(getattr(topic, "created_at", 0) or 0), int(getattr(topic, "id", -1) or -1)) in key_set:
                results.append(topic)
        return results
    
    def get_topic_by_name(self, topic_name: str) -> Optional[Topic]:
        name = str(topic_name or "").strip()
        if not name:
            return None
        for topic in self._topics:
            if str(topic.topic or "").strip() == name:
                return topic
        return None


class TopicAppService:
    def _init_topic_persist_queue(self):
        # 队列批量消费，区分add/update
        def consume_batch(topics: List[Topic]):
            to_update = [t for t in topics if int(getattr(t, 'id', 0) or 0) > 0]
            to_add = [t for t in topics if int(getattr(t, 'id', 0) or 0) <= 0]
            if to_update:
                self.topic_domain_service.update_topics(to_update)
            if to_add:
                self.topic_domain_service.add_topics(to_add)

        self._topic_persist_queue = CommonThreadPool.create_queue_batch_manager(
            batch_size=20, consume_batch=consume_batch
        )

    def put_topic_to_persist_queue(self, topic: Topic):
        self._topic_persist_queue.put(topic)

    def close_topic_persist_queue(self):
        self._topic_persist_queue.close_and_wait()

    @staticmethod
    def _normalize_search_text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _topic_name_matches(candidate: str, query: str) -> bool:
        candidate_text = str(candidate or "").strip()
        query_text = str(query or "").strip()
        if not candidate_text or not query_text:
            return False
        return candidate_text == query_text

    @staticmethod
    def _keyword_dedup_score(item: NewsKeyword) -> tuple[float, int]:
        return (float(getattr(item, "weigh", 0.0) or 0.0), int(getattr(item, "last_time", 0) or 0))

    @staticmethod
    def _entity_dedup_score(item: Entity) -> tuple[float, int]:
        return (float(getattr(item, "weigh", 0.0) or 0.0), int(getattr(item, "last_time", 0) or 0))

    @staticmethod
    def _fuzzy_text_match(candidate: str, query: str) -> bool:
        candidate_text = str(candidate or "").strip().lower()
        query_text = str(query or "").strip().lower()
        if not candidate_text or not query_text:
            return False
        return (
            candidate_text == query_text
            or query_text in candidate_text
            or candidate_text in query_text
        )

    def search_keywords_by_query(
        self,
        query: str,
        news_first_time: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[NewsKeyword]:
        """按 query 在时间范围内模糊检索 Keyword，并按 term 去重。"""
        normalized_query = self._normalize_search_text(query)
        if not normalized_query:
            return []

        if start_time is None:
            start_time = 0
        if end_time is None:
            end_time = int(time.time())

        matched = self.news_domain_service.get_keywords_by_fuzzy_term(
            term_query=normalized_query,
            news_first_time=news_first_time,
            start_time=int(start_time),
            end_time=int(end_time),
        )

        dedup_map: Dict[str, NewsKeyword] = {}
        for item in matched:
            term_key = str(item.term or "").strip().lower()
            if not term_key:
                continue
            existing = dedup_map.get(term_key)
            if existing is None or self._keyword_dedup_score(item) >= self._keyword_dedup_score(existing):
                dedup_map[term_key] = item

        deduped = list(dedup_map.values())
        deduped.sort(key=lambda x: self._keyword_dedup_score(x), reverse=True)
        return deduped[: max(1, int(limit))]

    def search_entities_by_query(
        self,
        query: str,
        news_first_time: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[Entity]:
        """按 query 在时间范围内模糊检索 Entity，并按 name 去重。"""
        normalized_query = self._normalize_search_text(query)
        if not normalized_query:
            return []

        if start_time is None:
            start_time = 0
        if end_time is None:
            end_time = int(time.time())

        matched = self.news_domain_service.get_entities_by_fuzzy_name(
            name_query=normalized_query,
            news_first_time=news_first_time,
            start_time=int(start_time),
            end_time=int(end_time),
        )

        dedup_map: Dict[str, Entity] = {}
        for item in matched:
            name_key = str(item.name or "").strip().lower()
            if not name_key:
                continue
            existing = dedup_map.get(name_key)
            if existing is None or self._entity_dedup_score(item) >= self._entity_dedup_score(existing):
                dedup_map[name_key] = item

        deduped = list(dedup_map.values())
        deduped.sort(key=lambda x: self._entity_dedup_score(x), reverse=True)
        return deduped[: max(1, int(limit))]

    def _find_existing_topic_by_keyword(self, keyword: str) -> Optional[Topic]:
        normalized_keyword = self._normalize_search_text(keyword)
        if not normalized_keyword:
            return None

        for topic in self.topic_cache_manager.get_topics():
            if self._topic_name_matches(topic.topic, normalized_keyword):
                return topic

        topic = self.topic_domain_service.find_recent_topic_by_name(
            topic_name=normalized_keyword,
            days_lookback=self.default_days_lookback,
        )
        if topic is not None and self._topic_name_matches(topic.topic, normalized_keyword):
            return topic
        return None

    def get_topic_by_keyword_or_build(
        self,
        keyword: str,
        news_first_time: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Optional[Topic]:
        """根据关键词返回现有 Topic，或基于匹配到的 Keyword/Entity 构建 Topic。"""
        normalized_keyword = self._normalize_search_text(keyword)
        if not normalized_keyword:
            return None

        existing_topic = self._find_existing_topic_by_keyword(normalized_keyword)
        if existing_topic is not None:
            return existing_topic

        matched_keywords = self.news_domain_service.get_keywords_by_terms(
            [normalized_keyword],
            news_first_time=news_first_time,
            start_time=start_time,
            end_time=end_time,
        )
        matched_entities = self.news_domain_service.get_entities_by_names(
            [normalized_keyword],
            news_first_time=news_first_time,
            start_time=start_time,
            end_time=end_time,
        )

        if not matched_keywords and not matched_entities:
            return None

        keyword_items = self.news_domain_service.get_news_list_by_keywords(
            keywords=matched_keywords,
            news_first_time=news_first_time,
            start_time=start_time,
            end_time=end_time,
        )
        entity_items = self.news_domain_service.get_news_list_by_entities(
            entities=matched_entities,
            news_first_time=news_first_time,
            start_time=start_time,
            end_time=end_time,
        )

        combined_items: Dict[tuple[str, str], NewsItem] = {}
        for item in [*keyword_items, *entity_items]:
            if not isinstance(item, NewsItem):
                continue
            key = (str(item.source_id or "").strip(), str(item.title or "").strip())
            if key == ("", ""):
                continue
            combined_items[key] = item

        if not combined_items:
            return None

        topic_name = normalized_keyword
        if matched_keywords:
            topic_name = str(matched_keywords[0].term or normalized_keyword).strip() or normalized_keyword
        elif matched_entities:
            topic_name = str(matched_entities[0].name or normalized_keyword).strip() or normalized_keyword

        return self.topic_domain_service.build_topic_from_news_items(
            topic_name=topic_name,
            news_items=list(combined_items.values()),
        )

    def __init__(
        self,
        topic_domain_service: TopicDomainService,
        news_domain_service: NewsDomainService,
        crawl_interval_seconds: int,
        topic_config: Optional[Dict[str, Any]] = None,
        topic_cache_manager: Optional[TopicCacheManager] = None,
        llm_title_analyzer: Optional[LLMAnalyzer] = None,
        first_time_lookback_seconds: int = 7 * 24 * 3600,
    ) -> None:
        self.topic_domain_service = topic_domain_service
        self.news_domain_service = news_domain_service
        self.crawl_interval_seconds = max(60, int(crawl_interval_seconds))
        self.topic_config = topic_config or {}
        self.topic_lookback = self._resolve_topic_lookback_window()
        self.topic_cache_manager = topic_cache_manager or TopicCacheManager_Memory()
        self.llm_title_analyzer = llm_title_analyzer
        self.event_manager = EventManager()
        self.common_thread_pool = CommonThreadPool()
        self.first_time_lookback_seconds = max(60, int(first_time_lookback_seconds))
        self._init_topic_persist_queue()

        # 注入所有默认值
        topic_conf = self.topic_config
        self.default_top_n = int(topic_conf.get("default_top_n", 60))
        self.default_cache_limit = int(topic_conf.get("default_cache_limit", 60))
        self.default_history_limit = int(topic_conf.get("default_history_limit", 100))
        # days_lookback自动由first_time_lookback_seconds计算
        self.days_lookback_days = max(1, int(self.first_time_lookback_seconds // 86400))
        self.default_days_lookback = int(topic_conf.get("default_days_lookback", self.days_lookback_days))

    def _resolve_topic_lookback_window(self) -> int:
        raw_hours = (self.topic_config or {}).get("lookback_hours")
        if raw_hours is not None:
            try:
                hours = float(raw_hours)
                if hours > 0:
                    return int(hours * 3600)
            except (TypeError, ValueError):
                pass

        fallback_seconds = self.crawl_interval_seconds * _TOPIC_LOOKBACK_MULTIPLIER
        return int(fallback_seconds)

    def build_topics_by_keywords_entities(
        self,
        keywords: List[NewsKeyword],
        entities: List[Entity],
        news_first_time: Optional[int],
    ) -> List[Topic]:
        """
        - 输入 keyword id list、entity id list
        - 从 news domain 按 keyword/entity id 找到相关 news items
        - 调用 build_topic_from_news_items 返回 topic list
        """
        if not keywords and not entities:
            return []

        topic_news_map: Dict[str, List[NewsItem]] = defaultdict(list)
        topic_item_ids_map: Dict[str, set[tuple[int, int]]] = defaultdict(set)

        if keywords:
            keyword_news_items = self.news_domain_service.get_news_list_by_keywords(
                keywords=keywords,
                news_first_time=news_first_time,
            )
            keyword_item_map: Dict[tuple[int, int], NewsItem] = {
                (int(item.id), int(item.first_time))
                : item
                for item in keyword_news_items
                if int(item.id or -1) > 0 and item.first_time is not None
            }

            # 以输入 keyword.term 为准分组，支持 term 在输入列表中重复出现。
            for keyword in keywords:
                if not isinstance(keyword, NewsKeyword):
                    continue
                topic_name = str(keyword.term or "").strip()
                if not topic_name:
                    continue
                if keyword.news_item_id is None or keyword.news_first_time is None:
                    continue

                item_key = (int(keyword.news_item_id), int(keyword.news_first_time))
                item = keyword_item_map.get(item_key)
                if item is None:
                    continue
                if item_key in topic_item_ids_map[topic_name]:
                    continue

                topic_item_ids_map[topic_name].add(item_key)
                topic_news_map[topic_name].append(item)

        if entities:
            entity_news_items = self.news_domain_service.get_news_list_by_entities(
                entities=entities,
                news_first_time=news_first_time,
            )
            entity_item_map: Dict[tuple[int, int], NewsItem] = {
                (int(item.id), int(item.first_time))
                : item
                for item in entity_news_items
                if int(item.id or -1) > 0 and item.first_time is not None
            }

            # 以输入 entity.name 为准分组，支持同名实体在输入列表中重复出现。
            for entity in entities:
                if not isinstance(entity, Entity):
                    continue
                topic_name = str(entity.name or "").strip()
                if not topic_name:
                    continue
                if entity.news_item_id is None or entity.news_first_time is None:
                    continue

                item_key = (int(entity.news_item_id), int(entity.news_first_time))
                item = entity_item_map.get(item_key)
                if item is None:
                    continue
                if item_key in topic_item_ids_map[topic_name]:
                    continue

                topic_item_ids_map[topic_name].add(item_key)
                topic_news_map[topic_name].append(item)

        if not topic_news_map:
            return []

        topics: List[Topic] = []
        for topic_name, items in topic_news_map.items():
            topic = self.topic_domain_service.build_topic_from_news_items(topic_name=topic_name, news_items=items)
            topics.append(topic)

        topics.sort(key=lambda t: t.total_weight, reverse=True)
        return topics

    @singleton_task()
    def recommend_and_cache_topics(
        self,
        start_time: Optional[int],
        end_time: Optional[int],
        news_first_time: Optional[int] = None,
        top_n: int = None,
        cache_limit: int = None,
    ) -> List[Topic]:
        # --- MyRedis 计时器逻辑，5分钟内重复调用直接 return ---
        try:
            from SentimentAnalyzeServer.system.infra import MyRedis
        except ImportError:
            from system.infra import MyRedis
        """
        - start_time/end_time: keyword/entity 的 last_time 查询区间
        - news_first_time: NewsItem 分区下界（默认: 当前时间 - topic 默认窗口）
        - 调用方法二构建 topic 列表
        - 通过 TopicCacheManager 保存
        """
        redis = MyRedis()
        redis_key = "recommend_and_cache_topics:last_run"
        cooldown = 300  # 5分钟
        now_ts = int(time.time())

        is_locked = redis.set(redis_key, now_ts, ttl_seconds=cooldown, nx=True)
        if not is_locked:
            logger.info(f"recommend_and_cache_topics: 距离上次执行不足5分钟，直接返回，无需重复执行。now={now_ts}")
            return []

        resolved_end_time = int(end_time) if end_time is not None else int(time.time())
        resolved_start_time = int(start_time) if start_time is not None else (resolved_end_time - self.topic_lookback)
        if resolved_start_time > resolved_end_time:
            resolved_start_time = resolved_end_time - self.topic_lookback

        resolved_news_first_time = (
            int(news_first_time)
            if news_first_time is not None
            else (int(time.time()) - self.topic_lookback)
        )
        if resolved_news_first_time > resolved_end_time:
            resolved_news_first_time = resolved_end_time - self.topic_lookback

        # 使用默认配置
        if top_n is None:
            top_n = self.default_top_n
        if cache_limit is None:
            cache_limit = self.default_cache_limit
        recommended_keywords, recommended_entities = self.news_domain_service.recommend_hot_term_items_by_time_range(
            start_time=resolved_start_time,
            end_time=resolved_end_time,
            news_first_time=resolved_news_first_time,
            top_n=top_n,
        )

        topics = self.build_topics_by_keywords_entities(
            keywords=recommended_keywords,
            entities=recommended_entities,
            news_first_time=resolved_news_first_time,
        )


        updated_topics: List[Topic] = []
        for new_status_topic in topics:
            new_status_topic.updated_at = int(time.time())
            # 1. 先根据topic name查找缓存或DB中的Topic（Topic DB）
            topic_name = str(new_status_topic.topic or "").strip()
            topic_db_match = self.topic_cache_manager.get_topic_by_name(topic_name)
            if topic_db_match is None:
                topic_db_match = self.topic_domain_service.find_recent_topic_by_name(topic_name, days_lookback=self.default_days_lookback)
            # 2. 用Topic DB主键查topic_metrics_history
            if topic_db_match is not None: # update 操作
                new_status_topic = self.topic_domain_service.applyNewStatus(new_status_topic, topic_db_match)
                #计算heat_change_percent和stage
                history = self.topic_domain_service.get_topic_history(new_status_topic)
                if not history:
                    logger.warning(
                        "话题没有历史数据. topic_name=%s, topic_id=%s",
                        topic_name,
                        topic_db_match.id,
                    )
                    self.put_topic_to_persist_queue(new_status_topic)
                    continue
                if (new_status_topic.updated_at - history[-1].updated_at) < 600: #小于5分钟，更新太快
                    #再防一手更新太快的异常状况，并log

                    logger.warning(
                        "话题更新太快，可能导致heat_change_percent和stage计算不准确. topic_name=%s, topic_id=%s, updated_at=%s, last_history_updated_at=%s",
                        topic_name,
                        topic_db_match.id,
                        new_status_topic.updated_at,
                        history[-1].updated_at if history else None,
                    )
                    continue
                new_status_topic = self.topic_domain_service.calculate_heat_change_and_stage(new_status_topic, history)
            # 生产者：入队，由消费者批量add/update
            self.put_topic_to_persist_queue(new_status_topic)
            updated_topics.append(new_status_topic)

        self.topic_cache_manager.save_or_update_topics_cache(updated_topics, limit=cache_limit)

        logger.info(
            "Topic recommendation cached successfully. topic_count=%s, top_n=%s, cache_limit=%s",
            len(updated_topics),
            top_n,
            cache_limit,
        )

        self.event_manager.publish(
            EVENT_TOPIC_RANK_UPDATED,
            {
                "topic_count": len(updated_topics),
                "topics": list(updated_topics),
                "top_n": int(top_n),
                "cache_limit": int(cache_limit),
                "start_time": int(resolved_start_time),
                "end_time": int(resolved_end_time),
                "news_first_time": int(resolved_news_first_time),
            },
        )
        
        return updated_topics

    def get_topic_snapshot_detail(
        self,
        topic_created_at: int,
        topic_id: int,
        history_limit: int = None,
    ) -> Result:
        cached_topic = self.topic_cache_manager.get_topic_by_composite_key(
            topic_created_at=topic_created_at,
            topic_id=topic_id,
        )

        if history_limit is None:
            history_limit = self.default_history_limit
        timeline = self.topic_domain_service.get_topic_timeline_and_latest(
            topic_created_at=topic_created_at,
            topic_id=int(topic_id),
            history_limit=max(1, int(history_limit)),
        )

        if cached_topic is not None:
            cache_key = (int(cached_topic.created_at or 0), int(cached_topic.id or -1))
            replaced = False
            for idx, item in enumerate(timeline):
                item_key = (int(item.created_at or 0), int(item.id or -1))
                if item_key == cache_key:
                    timeline[idx] = cached_topic
                    replaced = True
                    break
            if not replaced:
                timeline.insert(0, cached_topic)

        if not timeline:
            return Result.failure_result("未找到对应的Topic快照")

        payload = {
            "topic": timeline[0].to_dict(),
            "timeline": [entry.to_dict() for entry in timeline],
        }
        return Result.success_result(payload)

    def get_trending_topics(self) -> Result:
        cache_topics = self.topic_cache_manager.get_topics()
        if cache_topics:
            return Result.success_result(cache_topics)
        # 无缓存时，查库
        now = int(time.time())
        # 默认查最近6小时
        updated_at_start = now - self.topic_lookback
        topics = self.topic_domain_service.list_topics_by_time_range(
            created_at_start=now - self.first_time_lookback_seconds,
            updated_at_start=updated_at_start,
            updated_at_end=now,
            limit=self.default_top_n,
        )
        if topics:
            self.topic_cache_manager.save_or_update_topics_cache(topics, limit=self.default_cache_limit)
            return Result.success_result(topics)
        else:
            self.common_thread_pool.submit(self.recommend_and_cache_topics)
            return Result.failure_result("没有找到热门话题，系统正在重新计算")

    def get_topics_by_composite_keys(self, keys: List[tuple[int, int]]) -> Result:
        if not keys:
            return Result.success_result([])
            
        # 1. Try cache
        cached = self.topic_cache_manager.get_topic_by_composite_keys(keys)
        results = { (int(item.created_at), int(item.id)): item for item in cached }
        
        missing_keys = [k for k in keys if k not in results]
        
        # 2. Try DB for missing
        if missing_keys:
            for created_at, topic_id in missing_keys:
                topic = self.topic_domain_service.get_topic_timeline_and_latest(created_at, topic_id, history_limit=1)
                if topic:
                    results[(int(topic[0].created_at), int(topic[0].id))] = topic[0]
        
        # Keep original order
        final_list = []
        for key in keys:
            if key in results:
                final_list.append(results[key])
                
        return Result.success_result(final_list)

    @staticmethod
    def _is_blank_text(value: Optional[str]) -> bool:
        return not str(value or "").strip()

    @staticmethod
    def _topic_composite_key(topic: Topic) -> tuple[int, int]:
        return (int(getattr(topic, "created_at", 0) or 0), int(getattr(topic, "id", -1) or -1))

    @staticmethod
    def _extract_topic_titles(topic: Topic, max_titles: int = 50) -> List[str]:
        title_set: set[str] = set()
        for items in (topic.rank_data or {}).values():
            if not isinstance(items, list):
                continue
            for item in items:
                title = str(getattr(item, "title", "") or "").strip()
                if title:
                    title_set.add(title)
                if len(title_set) >= max_titles:
                    return list(title_set)
        return list(title_set)

    async def _summarize_llm_title_for_topic(self, topic: Topic) -> str:
        if self.llm_title_analyzer is None:
            return ""
        titles = self._extract_topic_titles(topic)
        if not titles:
            return ""
        
        result = await self.llm_title_analyzer.summarize_topic_title(topic.topic, titles, topic=topic)
        return str(result or "").strip()

    async def backfill_missing_llm_titles(self, limit: int = 50) -> Dict[str, Any]:
        if self.llm_title_analyzer is None:
            return {
                "success": False,
                "reason": "llm_title_analyzer_not_configured",
                "updated_count": 0,
                "candidate_count": 0,
            }
        
        # 暂时不直接查DB了，改为从Cache里找符合条件的Topic进行补全，避免DB查询性能问题。后续如有需要再改回DB查询。
        # db_candidates = self.topic_domain_service.list_topics_missing_llm_title(limit=max(1, int(limit)))
        # if not db_candidates:
        #     return {
        #         "success": True,
        #         "updated_count": 0,
        #         "candidate_count": 0,
        #     }

        cache_topics = self.topic_cache_manager.get_topics()
        cache_map = {self._topic_composite_key(topic): topic for topic in cache_topics}

        candidates: List[Topic] = [
            topic
            for topic in cache_topics
            if self.topic_domain_service.should_summarize_llm_title(topic)
        ]
        # for db_topic in db_candidates:   # DB暂无关联NewsItem数据，先使用Cache数据，后续如有需要再改为DB数据
        #     key = self._topic_composite_key(db_topic)
        #     cached = cache_map.get(key)
        #     # Prefer cache copy because it is more likely to contain fresh rank_data for LLM summarization.
        #     candidates.append(cached if cached is not None else db_topic)

        if not candidates:
            return {
                "success": True,
                "candidate_count": 0,
                "updated_count": 0,
            }

        # 使用 asyncio.gather 并发执行协程
        tasks = [self._summarize_llm_title_for_topic(topic) for topic in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        updated_count = 0
        skipped_count = 0
        cache_updated_count = 0

        for topic, result in zip(candidates, results):
            if isinstance(result, Exception):
                skipped_count += 1
                logger.error(
                    "build llm_title failed. created_at=%s, id=%s, topic=%s, error=%s",
                    topic.created_at, topic.id, topic.topic, str(result)
                )
                continue

            llm_title = str(result or "").strip()
            if not llm_title:
                skipped_count += 1
                continue

            updated = self.topic_domain_service.update_topic_llm_title_only(
                topic_created_at=int(topic.created_at or 0),
                topic_id=int(topic.id or -1),
                llm_title=llm_title,
            )
            if not updated:
                skipped_count += 1
                continue

            topic.llm_title = llm_title
            cache_topic = cache_map.get(self._topic_composite_key(topic))
            if cache_topic is not None:
                cache_topic.llm_title = llm_title
                cache_updated_count += 1

            updated_count += 1

        if cache_topics and cache_updated_count > 0:
            self.topic_cache_manager.save_or_update_topics_cache(cache_topics, limit=max(1, len(cache_topics)))

        return {
            "success": True,
            "candidate_count": len(candidates),
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "cache_updated_count": cache_updated_count,
        }

        if cache_topics and cache_updated_count > 0:
            self.topic_cache_manager.save_or_update_topics_cache(cache_topics, limit=max(1, len(cache_topics)))

        return {
            "success": True,
            "candidate_count": len(candidates),
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "cache_updated_count": cache_updated_count,
        }