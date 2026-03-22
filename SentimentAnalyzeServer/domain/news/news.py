from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set

@dataclass
class Entity:
    name: str
    type: str

@dataclass
class Keyword:
    term: str
    importance: float

@dataclass
class NewsItem:
    """新闻条目数据模型（热榜数据）"""

    title: str = ""                          # 新闻标题
    source_id: str = ""                      # 来源平台ID（如 toutiao, baidu）
    source_name: str = ""                    # 来源平台名称（运行时使用，数据库不存储）
    event_type: str = ""
    summary: str = ""
    entities: List[Entity] = field(default_factory=list)
    keywords: List[Keyword] = field(default_factory=list)
    latest_rank: int = 0                 # 排名
    url: str = ""                       # 链接 URL
    mobile_url: str = ""                # 移动端 URL
    crawl_time: str = ""                # 抓取时间（HH:MM 格式）
    # 展开后的情感分析字段 (Sentiment Analysis flattened)
    sentiment_polarity: str = ""
    positive_ratio: float = 0.0
    negative_ratio: float = 0.0
    neutral_ratio: float = 0.0
    # dimensions
    optimism_score: float = 0.0
    trust_score: float = 0.0
    controversy_score: float = 0.0
    attention_score: float = 0.0
    # 统计信息（用于分析）
    first_time: str = ""                # 首次出现时间
    last_time: str = ""                 # 最后出现时间
    analyzed_time: Optional[str] = None         # 分析时间
    count: int = 1                      # 出现次数
    rank_timeline: List[Dict[str, Any]] = field(default_factory=list)  # 完整排名时间线
                                        # 格式: [{"time": "09:30", "rank": 1}, {"time": "10:00", "rank": 2}, ...]
                                        # None 表示脱榜: [{"time": "11:00", "rank": None}]

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "title": self.title,
            "source_id": self.source_id,
            "source_name": self.source_name,
            "event_type": self.event_type,
            "summary": self.summary,
            "entities": [{"name": entity.name, "type": entity.type} for entity in self.entities],
            "keywords": [{"term": keyword.term, "importance": keyword.importance} for keyword in self.keywords],
            "latest_rank": self.latest_rank,
            "url": self.url,
            "mobile_url": self.mobile_url,
            "crawl_time": self.crawl_time,
            "sentiment_polarity": self.sentiment_polarity,
            "positive_ratio": self.positive_ratio,
            "negative_ratio": self.negative_ratio,
            "neutral_ratio": self.neutral_ratio,
            "optimism_score": self.optimism_score,
            "trust_score": self.trust_score,
            "controversy_score": self.controversy_score,
            "attention_score": self.attention_score,
            "first_time": self.first_time,
            "last_time": self.last_time,
            "analyzed_time": self.analyzed_time,
            "count": self.count,
            "rank_timeline": self.rank_timeline,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NewsItem":
        """从字典创建"""
        raw_entities = data.get("entities", [])
        raw_keywords = data.get("keywords", [])
        legacy_ranks = data.get("ranks", [])
        latest_rank = data.get("latest_rank")
        if latest_rank is None:
            latest_rank = data.get("rank", 0)
        if not latest_rank and legacy_ranks:
            latest_rank = legacy_ranks[0]

        return cls(
            title=data.get("title", ""),
            source_id=data.get("source_id", ""),
            source_name=data.get("source_name", ""),
            event_type=data.get("event_type", ""),
            summary=data.get("summary", ""),
            entities=[Entity(name=item.get("name", ""), type=item.get("type", "")) for item in raw_entities],
            keywords=[
                Keyword(term=item.get("term", ""), importance=float(item.get("importance", 0.0)))
                for item in raw_keywords
            ],
            latest_rank=int(latest_rank or 0),
            url=data.get("url", ""),
            mobile_url=data.get("mobile_url", ""),
            crawl_time=data.get("crawl_time", ""),
            sentiment_polarity=data.get("sentiment_polarity", ""),
            positive_ratio=float(data.get("positive_ratio", 0.0)),
            negative_ratio=float(data.get("negative_ratio", 0.0)),
            neutral_ratio=float(data.get("neutral_ratio", 0.0)),
            optimism_score=float(data.get("optimism_score", 0.0)),
            trust_score=float(data.get("trust_score", 0.0)),
            controversy_score=float(data.get("controversy_score", 0.0)),
            attention_score=float(data.get("attention_score", 0.0)),
            first_time=data.get("first_time", ""),
            last_time=data.get("last_time", ""),
            analyzed_time=data.get("analyzed_time"),
            count=data.get("count", 1),
            rank_timeline=data.get("rank_timeline", []),
        )

@dataclass
class NewsData:
    """
    新闻数据集合

    结构:
    - date: 日期（YYYY-MM-DD）
    - crawl_time: 抓取时间（HH时MM分）
    - items: 按来源ID分组的新闻条目
    - id_to_name: 来源ID到名称的映射
    - failed_ids: 失败的来源ID列表
    """

    date: str                                   # 日期
    crawl_time: str                             # 抓取时间
    items: Dict[str, List[NewsItem]]            # 按来源分组的新闻
    id_to_name: Dict[str, str] = field(default_factory=dict)   # ID到名称映射
    failed_ids: List[str] = field(default_factory=list)        # 失败的ID

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        items_dict = {}
        for source_id, news_list in self.items.items():
            items_dict[source_id] = [item.to_dict() for item in news_list]

        return {
            "date": self.date,
            "crawl_time": self.crawl_time,
            "items": items_dict,
            "id_to_name": self.id_to_name,
            "failed_ids": self.failed_ids,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NewsData":
        """从字典创建"""
        items = {}
        items_data = data.get("items", {})
        for source_id, news_list in items_data.items():
            items[source_id] = [NewsItem.from_dict(item) for item in news_list]

        return cls(
            date=data.get("date", ""),
            crawl_time=data.get("crawl_time", ""),
            items=items,
            id_to_name=data.get("id_to_name", {}),
            failed_ids=data.get("failed_ids", []),
        )

    def get_total_count(self) -> int:
        """获取新闻总数"""
        return sum(len(news_list) for news_list in self.items.values())

    def merge_with(self, other: "NewsData") -> "NewsData":
        """
        合并另一个 NewsData 到当前数据

        合并规则:
        - 相同 source_id + title 的新闻合并排名历史
        - 更新 last_time 和 count
        - 保留较早的 first_time
        """
        merged_items = {}

        # 复制当前数据
        for source_id, news_list in self.items.items():
            merged_items[source_id] = {item.title: item for item in news_list}

        # 合并其他数据
        for source_id, news_list in other.items.items():
            if source_id not in merged_items:
                merged_items[source_id] = {}

            for item in news_list:
                if item.title in merged_items[source_id]:
                    # 合并已存在的新闻
                    existing = merged_items[source_id][item.title]

                    # 合并排名时间线
                    combined_timeline = existing.rank_timeline + item.rank_timeline
                    existing.rank_timeline = sorted(
                        combined_timeline,
                        key=lambda x: (x.get("time", ""), x.get("rank") is None, x.get("rank", 0)),
                    )
                    if item.latest_rank:
                        existing.latest_rank = item.latest_rank

                    # 更新时间
                    if item.first_time and (not existing.first_time or item.first_time < existing.first_time):
                        existing.first_time = item.first_time
                    if item.last_time and (not existing.last_time or item.last_time > existing.last_time):
                        existing.last_time = item.last_time

                    # 更新计数
                    existing.count += item.count

                    # 合并结构化字段（只在缺失时补充）
                    if not existing.summary and item.summary:
                        existing.summary = item.summary
                    if not existing.event_type and item.event_type:
                        existing.event_type = item.event_type
                    if not existing.entities and item.entities:
                        existing.entities = item.entities
                    if not existing.keywords and item.keywords:
                        existing.keywords = item.keywords

                    # 保留URL（如果原来没有）
                    if not existing.url and item.url:
                        existing.url = item.url
                    if not existing.mobile_url and item.mobile_url:
                        existing.mobile_url = item.mobile_url
                else:
                    # 添加新新闻
                    merged_items[source_id][item.title] = item

        # 转换回列表格式
        final_items = {}
        for source_id, items_dict in merged_items.items():
            final_items[source_id] = list(items_dict.values())

        # 合并 id_to_name
        merged_id_to_name = {**self.id_to_name, **other.id_to_name}

        # 合并 failed_ids（去重）
        merged_failed_ids = list(set(self.failed_ids + other.failed_ids))

        return NewsData(
            date=self.date or other.date,
            crawl_time=other.crawl_time,  # 使用较新的抓取时间
            items=final_items,
            id_to_name=merged_id_to_name,
            failed_ids=merged_failed_ids,
        )


class StorageBackend(ABC):
    """
    存储后端抽象基类

    所有存储后端都需要实现这些方法，以支持:
    - 保存新闻数据
    - 读取当天所有数据
    - 检测新增新闻
    """

    @abstractmethod
    def save_news_data(self, data: NewsData) -> bool:
        """
        保存新闻数据

        Args:
            data: 新闻数据

        Returns:
            是否保存成功
        """
        pass

    @abstractmethod
    def get_today_all_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        """
        获取指定日期的所有新闻数据

        Args:
            date: 日期字符串（YYYY-MM-DD），默认为今天

        Returns:
            合并后的新闻数据，如果没有数据返回 None
        """
        pass

    @abstractmethod
    def get_latest_crawl_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        """
        获取最新一次抓取的数据

        Args:
            date: 日期字符串，默认为今天

        Returns:
            最新抓取的新闻数据
        """
        pass

    @abstractmethod
    def detect_new_titles(self, current_data: NewsData) -> Dict[str, Dict]:
        """
        检测新增的标题

        Args:
            current_data: 当前抓取的数据

        Returns:
            新增的标题数据，格式: {source_id: {title: title_data}}
        """
        pass

    @abstractmethod
    def is_first_crawl_today(self, date: Optional[str] = None) -> bool:
        """
        检查是否是当天第一次抓取

        Args:
            date: 日期字符串，默认为今天

        Returns:
            是否是第一次抓取
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """
        清理资源（如临时文件、数据库连接等）
        """
        pass

    @abstractmethod
    def cleanup_old_data(self, retention_days: int) -> int:
        """
        清理过期数据

        Args:
            retention_days: 保留天数（0 表示不清理）

        Returns:
            删除的日期目录数量
        """
        pass

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """
        存储后端名称
        """
        pass

    def record_period_execution(self, date_str: str, period_key: str, action: str) -> bool:
        """
        记录时间段的 action 执行

        Args:
            date_str: 日期字符串 YYYY-MM-DD
            period_key: 时间段 key
            action: 动作类型 (analyze / push)

        Returns:
            是否记录成功
        """
        return False

    def save_analyzed_news(self, news_ids: List[str], source_type: str, interests_file: str, prompt_hash: str, matched_ids: Set[str], date: Optional[str] = None) -> int:
        return 0

    def get_analyzed_news_ids(self, source_type: str = "hotlist", date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> Set[str]:
        return set()

    def clear_analyzed_news(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> int:
        return 0

    def clear_unmatched_analyzed_news(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> int:
        return 0

    def get_all_news_ids(self, date: Optional[str] = None) -> List[Dict]:
        return []


class NewsDomainService:
    def __init__(self, storage: StorageBackend) -> None:
        self.storage = storage

    def applyNewsField(self, src: NewsItem, target: NewsItem) -> NewsItem:
        target.title = src.title
        target.source_id = src.source_id
        target.source_name = src.source_name
        target.event_type = src.event_type
        target.summary = src.summary
        target.entities = [Entity(name=entity.name, type=entity.type) for entity in src.entities]
        target.keywords = [Keyword(term=keyword.term, importance=keyword.importance) for keyword in src.keywords]
        target.latest_rank = src.latest_rank
        target.url = src.url
        target.mobile_url = src.mobile_url
        target.crawl_time = src.crawl_time
        target.sentiment_polarity = src.sentiment_polarity
        target.positive_ratio = src.positive_ratio
        target.negative_ratio = src.negative_ratio
        target.neutral_ratio = src.neutral_ratio
        target.optimism_score = src.optimism_score
        target.trust_score = src.trust_score
        target.controversy_score = src.controversy_score
        target.attention_score = src.attention_score
        target.first_time = src.first_time
        target.last_time = src.last_time
        target.analyzed_time = src.analyzed_time
        target.count = src.count
        target.rank_timeline = [dict(point) for point in src.rank_timeline]
        return target

    def updateNews(self, date_str: str, item: NewsItem) -> bool:
        existing_data = self.storage.get_today_all_data(date_str)
        if not existing_data:
            return False

        existing_item: Optional[NewsItem] = None
        for news_list in existing_data.items.values():
            for candidate in news_list:
                if candidate.source_id == item.source_id and candidate.title == item.title:
                    existing_item = candidate
                    break
            if existing_item:
                break

        if not existing_item:
            return False

        updated = self.applyNewsField(item, existing_item)
        if not updated.crawl_time:
            updated.crawl_time = existing_item.crawl_time

        payload = NewsData(
            date=date_str,
            crawl_time=updated.crawl_time,
            items={updated.source_id: [updated]},
            id_to_name={updated.source_id: updated.source_name},
            failed_ids=[],
        )
        return self.storage.save_news_data(payload)

    def save_news_data(self, data: NewsData) -> bool:
        return self.storage.save_news_data(data)

    def get_today_all_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        return self.storage.get_today_all_data(date)

    def get_latest_crawl_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        return self.storage.get_latest_crawl_data(date)

    def detect_new_titles(self, current_data: NewsData) -> Dict[str, Dict]:
        return self.storage.detect_new_titles(current_data)

