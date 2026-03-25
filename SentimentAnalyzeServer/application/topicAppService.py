from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
import logging
from typing import Dict, List, Optional

from SentimentAnalyzeServer.domain.news.news import Entity, Keyword, NewsDomainService, NewsItem
from SentimentAnalyzeServer.domain.topic.topic import Topic, TopicDomainService
from application.common import Result


logger = logging.getLogger(__name__)


class TopicCacheManager(ABC):
    # 热门话题数量上限暂时定为15个

    @abstractmethod
    def save_topics(self, topics: List[Topic], limit: int = 15) -> None:
        pass

    @abstractmethod
    def get_topics(self) -> List[Topic]:
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

    @staticmethod
    def _topic_weight(topic: Topic) -> float:
        # Compatible with both Topic.total_weight and legacy total_weigh naming.
        return float(getattr(topic, "total_weight", getattr(topic, "total_weigh", 0.0)) or 0.0)

    @staticmethod
    def _topic_updated_at(topic: Topic) -> datetime:
        updated_at = getattr(topic, "updated_at", None)
        if isinstance(updated_at, datetime):
            return updated_at
        created_at = getattr(topic, "created_at", None)
        if isinstance(created_at, datetime):
            return created_at
        return datetime.min

    @classmethod
    def _eviction_score(cls, topic: Topic, now: datetime) -> float:
        updated_at = cls._topic_updated_at(topic)
        age_hours = max(0.0, (now - updated_at).total_seconds() / 3600.0) if updated_at != datetime.min else 10**9
        freshness = 1.0 / (1.0 + age_hours)
        return cls._topic_weight(topic) * freshness

    def save_topics(self, topics: List[Topic], limit: int = 15) -> None:
        capped_limit = max(1, limit)
        now = datetime.now()

        merged: Dict[str, Topic] = {}
        for topic in self._topics + list(topics):
            key = self._topic_key(topic)
            existing = merged.get(key)
            if existing is None:
                merged[key] = topic
                continue

            current_updated = self._topic_updated_at(existing)
            incoming_updated = self._topic_updated_at(topic)
            if incoming_updated > current_updated:
                merged[key] = topic
                continue
            if incoming_updated == current_updated and self._topic_weight(topic) > self._topic_weight(existing):
                merged[key] = topic

        sorted_topics = sorted(
            merged.values(),
            key=lambda t: (
                self._eviction_score(t, now),
                self._topic_updated_at(t),
                self._topic_weight(t),
            ),
            reverse=True,
        )
        self._topics = sorted_topics[:capped_limit]

    def get_topics(self) -> List[Topic]:
        return list(self._topics)


class TopicAppService:
    def __init__(
        self,
        topic_domain_service: TopicDomainService,
        news_domain_service: NewsDomainService,
        topic_cache_manager: Optional[TopicCacheManager] = None,
    ) -> None:
        self.topic_domain_service = topic_domain_service
        self.news_domain_service = news_domain_service
        self.topic_cache_manager = topic_cache_manager or TopicCacheManager_Memory()

    def build_topics_by_keyword_entity_ids(
        self,
        keyword_ids: List[int],
        entity_ids: List[int],
        start_time: datetime,
        end_time: datetime,
        keyword_terms: Optional[List[str]] = None,
        entity_names: Optional[List[str]] = None,
    ) -> List[Topic]:
        """
        - 输入 keyword id list、entity id list
        - 从 news domain 按 keyword/entity id 找到相关 news items
        - 调用 build_topic_from_news_items 返回 topic list
        """
        keyword_id_set = {int(i) for i in keyword_ids if int(i) > 0}
        entity_id_set = {int(i) for i in entity_ids if int(i) > 0}
        keyword_term_set = {str(term).strip() for term in (keyword_terms or []) if str(term).strip()}
        entity_name_set = {str(name).strip() for name in (entity_names or []) if str(name).strip()}

        if not keyword_id_set and not entity_id_set and not keyword_term_set and not entity_name_set:
            return []

        topic_news_map: Dict[str, List[NewsItem]] = defaultdict(list)
        topic_item_ids_map: Dict[str, set[int]] = defaultdict(set)

        if keyword_id_set:
            keyword_news_items = self.news_domain_service.get_news_list_by_keyword_ids(
                keyword_ids=list(keyword_id_set),
                start_time=start_time,
                end_time=end_time,
            )
            for item in keyword_news_items:
                for keyword in item.keywords:
                    topic_name = keyword.term.strip()
                    if not topic_name:
                        continue
                    if keyword_term_set:
                        if topic_name not in keyword_term_set:
                            continue
                    elif int(keyword.id) not in keyword_id_set:
                        continue
                    if int(item.id) in topic_item_ids_map[topic_name]:
                        continue
                    topic_item_ids_map[topic_name].add(int(item.id))
                    topic_news_map[topic_name].append(item)

        if entity_id_set:
            entity_news_items = self.news_domain_service.get_news_list_by_entity_ids(
                entity_ids=list(entity_id_set),
                start_time=start_time,
                end_time=end_time,
            )
            for item in entity_news_items:
                for entity in item.entities:
                    topic_name = entity.name.strip()
                    if not topic_name:
                        continue
                    if entity_name_set:
                        if topic_name not in entity_name_set:
                            continue
                    elif int(entity.id) not in entity_id_set:
                        continue
                    if int(item.id) in topic_item_ids_map[topic_name]:
                        continue
                    topic_item_ids_map[topic_name].add(int(item.id))
                    topic_news_map[topic_name].append(item)

        if not topic_news_map:
            return []

        topics: List[Topic] = []
        for topic_name, items in topic_news_map.items():
            topic = self.topic_domain_service.build_topic_from_news_items(topic_name=topic_name, news_items=items)
            topics.append(topic)

        topics.sort(key=lambda t: t.total_weight, reverse=True)
        return topics

    def recommend_and_cache_topics(
        self,
        start_time: datetime,
        end_time: datetime,
        top_n: int = 10,
        cache_limit: int = 15,
    ) -> List[Topic]:
        """
        - 调用方法二构建 topic 列表
        - 通过 TopicCacheManager 保存
        """
        recommended_keywords, recommended_entities = self.news_domain_service.recommend_hot_terms_by_time_range(
            start_time=start_time,
            end_time=end_time,
            top_n=top_n,
        )

        keyword_ids = [int(keyword.id) for keyword in recommended_keywords if int(keyword.id) > 0]
        entity_ids = [int(entity.id) for entity in recommended_entities if int(entity.id) > 0]
        keyword_terms = [keyword.term.strip() for keyword in recommended_keywords if keyword.term and keyword.term.strip()]
        entity_names = [entity.name.strip() for entity in recommended_entities if entity.name and entity.name.strip()]

        topics = self.build_topics_by_keyword_entity_ids(
            keyword_ids=keyword_ids,
            entity_ids=entity_ids,
            keyword_terms=keyword_terms,
            entity_names=entity_names,
            start_time=start_time,
            end_time=end_time,
        )

        self.topic_cache_manager.save_topics(topics, limit=cache_limit)

        logger.info(
            "Topic recommendation cached successfully. topic_count=%s, top_n=%s, cache_limit=%s",
            len(topics),
            top_n,
            cache_limit,
        )
        
        return topics

    def get_trending_topics(self) -> Result:
        cache_topics = self.topic_cache_manager.get_topics()
        if cache_topics:
            return Result.success_result(cache_topics)
        else:
            now = datetime.now()
            start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = now
            self.recommend_and_cache_topics(
                start_time=start_time,
                end_time=end_time,
                top_n=10,
                cache_limit=15,
            )
            refreshed_topics = self.topic_cache_manager.get_topics()
            if refreshed_topics:
                return Result.success_result(refreshed_topics)
            return Result.failure_result("没有找到热门话题，系统正在重新计算")