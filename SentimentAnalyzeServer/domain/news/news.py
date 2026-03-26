from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Set, Tuple

DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
)


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    text = str(value).strip()
    if not text:
        return None

    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def format_datetime(value: Optional[datetime], fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return value.strftime(fmt) if value else ""

@dataclass
class Entity:
    id: int = field(default=-1)
    name: str = ""
    type: str = ""
    weigh: float = 0.0

@dataclass
class Keyword:
    id: int = field(default=-1)
    term: str = ""
    importance: float = 0.0
    weigh: float = 0.0


@dataclass
class RankTimelineEntry:
    id: int = field(default=-1)
    time: Optional[datetime] = None
    rank: int = 0

@dataclass
class NewsItem:
    """新闻条目数据模型（热榜数据）"""

    id: int = field(default=-1)                             # 数据库主键ID
    title: str = ""                          # 新闻标题,（source_id + title）联合唯一
    source_id: str = ""                      # 来源平台ID（如 toutiao, baidu）
    source_name: str = ""                    # 来源平台名称（运行时使用，数据库不存储）
    event_type: str = ""
    summary: str = ""
    entities: List[Entity] = field(default_factory=list)
    keywords: List[Keyword] = field(default_factory=list)
    latest_rank: int = 0                 # 排名
    url: str = ""                       # 链接 URL
    mobile_url: str = ""                # 移动端 URL
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
    first_time: Optional[datetime] = None                # 首次出现时间
    last_time: Optional[datetime] = None                 # 最后出现时间
    analyzed_time: Optional[datetime] = None         # 分析时间
    total_weigh: float = 0.0            # 综合权重
    rank_timeline_obj: List[RankTimelineEntry] = field(default_factory=list)  # 完整排名时间线对象
                                        # 格式: [("09:30", 1), ("10:00", 2), ...]
                                        # rank <= 0 表示脱榜

    @property
    def rank_timeline(self) -> List[Tuple[datetime, int]]:
        """对外兼容旧结构，返回 (time, rank) 列表。"""
        return [(point.time, point.rank) for point in self.rank_timeline_obj if point.time is not None]

    @property
    def count(self) -> int:
        """出现次数由 rank_timeline 长度动态计算。"""
        return len(self.rank_timeline_obj)

    def deduplicate_entities_and_keywords(self) -> None:
        """去重 entities（按 name+type） 和 keywords（按 term）"""
        # 去重 entities: 保留首次出现的，保留最高权重
        seen_entities: Dict[Tuple[str, str], Entity] = {}
        for entity in self.entities:
            key = (entity.name.strip(), entity.type.strip())
            if key not in seen_entities or entity.weigh > seen_entities[key].weigh:
                seen_entities[key] = entity
        self.entities = list(seen_entities.values())

        # 去重 keywords: 保留首次出现的，保留最高权重
        seen_keywords: Dict[str, Keyword] = {}
        for keyword in self.keywords:
            key = keyword.term.strip()
            if key not in seen_keywords or keyword.weigh > seen_keywords[key].weigh:
                seen_keywords[key] = keyword
        self.keywords = list(seen_keywords.values())

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "title": self.title,
            "source_id": self.source_id,
            "source_name": self.source_name,
            "event_type": self.event_type,
            "summary": self.summary,
            "entities": [{"id": entity.id, "name": entity.name, "type": entity.type, "weigh": entity.weigh} for entity in self.entities],
            "keywords": [{"id": keyword.id, "term": keyword.term, "importance": keyword.importance, "weigh": keyword.weigh} for keyword in self.keywords],
            "latest_rank": self.latest_rank,
            "url": self.url,
            "mobile_url": self.mobile_url,
            "sentiment_polarity": self.sentiment_polarity,
            "positive_ratio": self.positive_ratio,
            "negative_ratio": self.negative_ratio,
            "neutral_ratio": self.neutral_ratio,
            "optimism_score": self.optimism_score,
            "trust_score": self.trust_score,
            "controversy_score": self.controversy_score,
            "attention_score": self.attention_score,
            "first_time": format_datetime(self.first_time, "%Y-%m-%d %H:%M"),
            "last_time": format_datetime(self.last_time, "%Y-%m-%d %H:%M"),
            "analyzed_time": format_datetime(self.analyzed_time),
            "count": self.count,
            "total_weigh": self.total_weigh,
            "rank_timeline": [
                {
                    "id": point.id,
                    "time": format_datetime(point.time, "%Y-%m-%d %H:%M") if point.time else "",
                    "rank": point.rank if point.rank > 0 else None,
                }
                for point in self.rank_timeline_obj
            ],
        }

    @staticmethod
    def _parse_rank_timeline(timeline_data: Any) -> List[RankTimelineEntry]:
        """解析 rank_timeline 数据，转换为 RankTimelineEntry 列表。"""
        if not timeline_data:
            return []

        result: List[RankTimelineEntry] = []
        for item in timeline_data:
            if isinstance(item, dict):
                try:
                    entry_id = int(item.get("id", -1) or -1)
                    entry_time = parse_datetime(item.get("time"))
                    entry_rank = int(item.get("rank") or 0)
                    if entry_time is None:
                        continue
                    result.append(RankTimelineEntry(id=entry_id, time=entry_time, rank=entry_rank))
                except (TypeError, ValueError):
                    continue
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                try:
                    entry_time = parse_datetime(item[0])
                    entry_rank = int(item[1])
                    if entry_time is None:
                        continue
                    result.append(RankTimelineEntry(time=entry_time, rank=entry_rank))
                except (TypeError, ValueError, IndexError):
                    continue

        return result

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
        legacy_time = data.get("crawl_time", "")
        last_time = parse_datetime(data.get("last_time")) or parse_datetime(legacy_time)

        return cls(
            id=int(data.get("id", -1) or -1),
            title=data.get("title", ""),
            source_id=data.get("source_id", ""),
            source_name=data.get("source_name", ""),
            event_type=data.get("event_type", ""),
            summary=data.get("summary", ""),
            entities=[
                Entity(
                    id=int(item.get("id", -1) or -1),
                    name=item.get("name", ""),
                    type=item.get("type", ""),
                    weigh=float(item.get("weigh", data.get("total_weigh", 0.0)) or 0.0),
                )
                for item in raw_entities
            ],
            keywords=[
                Keyword(
                    id=int(item.get("id", -1) or -1),
                    term=item.get("term", ""),
                    importance=float(item.get("importance", 0.0)),
                    weigh=float(item.get("weigh", data.get("total_weigh", 0.0)) or 0.0),
                )
                for item in raw_keywords
            ],
            latest_rank=int(latest_rank or 0),
            url=data.get("url", ""),
            mobile_url=data.get("mobile_url", ""),
            sentiment_polarity=data.get("sentiment_polarity", ""),
            positive_ratio=float(data.get("positive_ratio", 0.0)),
            negative_ratio=float(data.get("negative_ratio", 0.0)),
            neutral_ratio=float(data.get("neutral_ratio", 0.0)),
            optimism_score=float(data.get("optimism_score", 0.0)),
            trust_score=float(data.get("trust_score", 0.0)),
            controversy_score=float(data.get("controversy_score", 0.0)),
            attention_score=float(data.get("attention_score", 0.0)),
            first_time=parse_datetime(data.get("first_time")),
            last_time=last_time,
            analyzed_time=parse_datetime(data.get("analyzed_time")),
            total_weigh=float(data.get("total_weigh", 0.0)),
            rank_timeline_obj=cls._parse_rank_timeline(data.get("rank_timeline", [])),
        )

@dataclass
class NewsData:
    """
    新闻数据集合

    结构:
    - date: 日期（YYYY-MM-DD）
    - last_time: 最新抓取时间（YYYY-MM-DD HH:MM）
    - items: 按来源ID分组的新闻条目
    - id_to_name: 来源ID到名称的映射
    - failed_ids: 失败的来源ID列表
    """

    date: Optional[datetime]                              # 日期
    last_time: Optional[datetime]                         # 最新抓取时间
    items: Dict[str, List[NewsItem]]            # 按来源分组的新闻
    id_to_name: Dict[str, str] = field(default_factory=dict)   # ID到名称映射
    failed_ids: List[str] = field(default_factory=list)        # 失败的ID

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        items_dict = {}
        for source_id, news_list in self.items.items():
            items_dict[source_id] = [item.to_dict() for item in news_list]

        return {
            "date": format_datetime(self.date, "%Y-%m-%d"),
            "last_time": format_datetime(self.last_time, "%Y-%m-%d %H:%M"),
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
            date=parse_datetime(data.get("date")),
            last_time=parse_datetime(data.get("last_time")) or parse_datetime(data.get("crawl_time")),
            items=items,
            id_to_name=data.get("id_to_name", {}),
            failed_ids=data.get("failed_ids", []),
        )

    def get_total_count(self) -> int:
        """获取新闻总数"""
        return sum(len(news_list) for news_list in self.items.values())

    @staticmethod
    def _merge_rank_timeline(
        base_timeline: List[RankTimelineEntry],
        incoming_timeline: List[RankTimelineEntry],
    ) -> List[RankTimelineEntry]:
        """合并并去重 rank timeline，去重键为 (time, rank)。"""
        combined_timeline = (base_timeline or []) + (incoming_timeline or [])
        dedup_timeline: List[RankTimelineEntry] = []
        seen: Set[Tuple[datetime, int]] = set()

        for point in combined_timeline:
            if point.time is None:
                continue
            key = (point.time, point.rank)
            if key in seen:
                continue
            seen.add(key)
            dedup_timeline.append(point)

        return sorted(
            dedup_timeline,
            key=lambda x: (x.time, x.rank <= 0, x.rank if x.rank > 0 else 0),
        )

    def merge_duplicate_titles_by_source(self) -> int:
        """同一来源内按 title 合并重复新闻，仅合并 timeline。返回合并次数。"""
        merged_count = 0

        for source_id, news_list in self.items.items():
            title_map: Dict[str, NewsItem] = {}
            merged_list: List[NewsItem] = []

            for item in news_list:
                title_key = item.title
                if title_key not in title_map:
                    title_map[title_key] = item
                    merged_list.append(item)
                    continue

                existing_item = title_map[title_key]
                existing_item.rank_timeline_obj = self._merge_rank_timeline(
                    existing_item.rank_timeline_obj,
                    item.rank_timeline_obj,
                )

                # 合并后尽量保持时间字段和最新排名一致
                if item.first_time and (not existing_item.first_time or item.first_time < existing_item.first_time):
                    existing_item.first_time = item.first_time
                if item.last_time and (not existing_item.last_time or item.last_time > existing_item.last_time):
                    existing_item.last_time = item.last_time

                valid_points = [point for point in existing_item.rank_timeline_obj if point.rank > 0]
                if valid_points:
                    latest_point = max(valid_points, key=lambda p: p.time)
                    existing_item.latest_rank = latest_point.rank

                merged_count += 1

            self.items[source_id] = merged_list

        return merged_count

    
class NewsItemRepository(ABC):
    """
    存储后端抽象基类

    所有存储后端都需要实现这些方法，以支持:
    - 保存新闻数据
    - 读取当天所有数据
    - 检测新增新闻
    """

    @abstractmethod
    def add_news_items(self, news_list: List[NewsItem]) -> List[NewsItem]:
        """
        保存新闻数据

        Args:
            news_list: 新闻条目列表

        Returns:
            保存成功后的新闻条目列表
        """
        pass

    @abstractmethod
    def update_news_list(self, news_list: List[NewsItem]) -> bool:
        """根据 NewsItem.id 批量更新新闻数据。"""
        pass

    def update_crawled_news_list(self, news_list: List[NewsItem]) -> List[NewsItem]:
        """仅更新抓取引起变化的字段（例如 rank/url/last_time/timeline）。"""
        if self.update_news_list(news_list):
            return news_list
        return []

    @abstractmethod
    def get_news_list_by_source_title_list(self, source_title_list: List[tuple[str, str]]) -> List[NewsItem]:
        """根据 (source_id, title) 列表查询新闻数据。"""
        pass


    @abstractmethod
    def get_latest_crawl_data(self, date: Optional[datetime] = None) -> Optional[NewsData]:
        """
        获取最新一次抓取的数据

        Args:
            date: 日期字符串，默认为今天

        Returns:
            最新抓取的新闻数据
        """
        pass

    @abstractmethod
    def is_first_crawl_today(self, date: Optional[datetime] = None) -> bool:
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

    def record_period_execution(self, date_str: datetime, period_key: str, action: str) -> bool:
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

    def save_analyzed_news(self, news_ids: List[str], source_type: str, interests_file: str, prompt_hash: str, matched_ids: Set[str], date: Optional[datetime] = None) -> int:
        return 0

    def get_analyzed_news_ids(self, source_type: str = "hotlist", date: Optional[datetime] = None, interests_file: str = "ai_interests.txt") -> Set[str]:
        return set()

    def clear_analyzed_news(self, date: Optional[datetime] = None, interests_file: str = "ai_interests.txt") -> int:
        return 0

    def clear_unmatched_analyzed_news(self, date: Optional[datetime] = None, interests_file: str = "ai_interests.txt") -> int:
        return 0

    def get_all_news_ids(self, date: Optional[datetime] = None) -> List[Dict]:
        return []

    @abstractmethod
    def get_news_list_by_latest_crawl_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[List[NewsItem]]:
        pass

    @abstractmethod
    def get_news_list_by_first_time_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[List[NewsItem]]:
        pass

    @abstractmethod
    def get_keywords_by_last_time_range(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Keyword]:
        """根据 first_time 范围查询关键词表。"""
        pass

    @abstractmethod
    def get_entities_by_last_time_range(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Entity]:
        """根据 first_time 范围查询实体表。"""
        pass

    @abstractmethod
    def get_news_list_by_keyword_ids(
        self,
        keyword_ids: List[int],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[NewsItem]:
        """根据 keyword id 列表查询 NewsItem。"""
        pass

    @abstractmethod
    def get_news_list_by_entity_ids(
        self,
        entity_ids: List[int],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[NewsItem]:
        """根据 entity id 列表查询 NewsItem。"""
        pass


class NewsDomainService:
    def __init__(self, storage: NewsItemRepository) -> None:
        self.storage = storage
        self.rank_threshold: int = 10
        self.weight_config: Dict[str, float] = {
            "RANK_WEIGHT": 0.6,
            "FREQUENCY_WEIGHT": 0.3,
            "HOTNESS_WEIGHT": 0.1,
        }

    def _set_total_weight(self, item: NewsItem) -> float:
        """基于 rank_timeline 计算并设置综合权重。
        同步更新keywords/entities的weigh字段，默认使用total_weigh值。"""
        ranks: List[int] = []
        for _, rank in item.rank_timeline:
            if rank <= 0:
                continue
            ranks.append(rank)

        if not ranks:
            item.total_weigh = 0.0
            for keyword in item.keywords:
                keyword.weigh = item.total_weigh
            for entity in item.entities:
                entity.weigh = item.total_weigh
            return item.total_weigh

        count = item.count or len(ranks)

        rank_scores = []
        for rank in ranks:
            score = 11 - min(rank, 10)
            rank_scores.append(score)

        rank_weight = sum(rank_scores) / len(ranks) if ranks else 0
        frequency_weight = min(count, 10) * 10

        high_rank_count = sum(1 for rank in ranks if rank <= self.rank_threshold)
        hotness_ratio = high_rank_count / len(ranks) if ranks else 0
        hotness_weight = hotness_ratio * 100

        item.total_weigh = (
            rank_weight * float(self.weight_config.get("RANK_WEIGHT", 0.6))
            + frequency_weight * float(self.weight_config.get("FREQUENCY_WEIGHT", 0.3))
            + hotness_weight * float(self.weight_config.get("HOTNESS_WEIGHT", 0.1))
        )

        for keyword in item.keywords:
            keyword.weigh = item.total_weigh*keyword.importance
        for entity in item.entities:
            entity.weigh = item.total_weigh

        return item.total_weigh

    def recommend_hot_terms_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        top_n: int = 10,
    ) -> Tuple[List[Keyword], List[Entity]]:
        """
        - 通过 keyword 和 entity 推荐热点 topic 候选
        - 返回 (keyword_list, entity_list)
        """
        keywords = self.get_keywords_by_time_range(start_time, end_time)
        entities = self.get_entities_by_time_range(start_time, end_time)

        keyword_agg: Dict[str, Keyword] = {}
        for keyword in keywords:
            key = keyword.term.strip()
            if not key:
                continue

            if key not in keyword_agg:
                keyword_agg[key] = Keyword(
                    id=int(keyword.id) if int(keyword.id) > 0 else -1,
                    term=key,
                    importance=float(keyword.importance),
                    weigh=0.0,
                )

            agg_keyword = keyword_agg[key]
            agg_keyword.weigh += float(keyword.weigh)
            if int(keyword.id) > 0 and int(agg_keyword.id) <= 0:
                agg_keyword.id = int(keyword.id)

        entity_agg: Dict[str, Entity] = {}
        for entity in entities:
            key = entity.name.strip()
            if not key:
                continue

            if key not in entity_agg:
                entity_agg[key] = Entity(
                    id=int(entity.id) if int(entity.id) > 0 else -1,
                    name=key,
                    type=entity.type,
                    weigh=0.0,
                )

            agg_entity = entity_agg[key]
            agg_entity.weigh += float(entity.weigh)
            if int(entity.id) > 0 and int(agg_entity.id) <= 0:
                agg_entity.id = int(entity.id)

        # 去重：name 与 term 相同，保留 weigh 总和更高的一侧
        overlap_keys = set(keyword_agg.keys()) & set(entity_agg.keys())
        for key in overlap_keys:
            if float(keyword_agg[key].weigh) >= float(entity_agg[key].weigh):
                del entity_agg[key]
            else:
                del keyword_agg[key]

        sorted_keywords = [
            v
            for _, v in sorted(keyword_agg.items(), key=lambda x: float(x[1].weigh), reverse=True)
            if int(v.id) > 0
        ][: max(1, top_n)]

        sorted_entities = [
            v
            for _, v in sorted(entity_agg.items(), key=lambda x: float(x[1].weigh), reverse=True)
            if int(v.id) > 0
        ][: max(1, top_n)]

        return sorted_keywords, sorted_entities

    def applyNewsField(self, src: NewsItem, target: NewsItem) -> NewsItem:
        if src.title not in (None, ""):
            target.title = src.title
        if src.source_id not in (None, ""):
            target.source_id = src.source_id
        if src.source_name not in (None, ""):
            target.source_name = src.source_name
        if src.event_type not in (None, ""):
            target.event_type = src.event_type
        if src.summary not in (None, ""):
            target.summary = src.summary
        if src.entities:
            target.entities = [Entity(id=entity.id, name=entity.name, type=entity.type, weigh=entity.weigh) for entity in src.entities]
        if src.keywords:
            target.keywords = [Keyword(id=keyword.id, term=keyword.term, importance=keyword.importance, weigh=keyword.weigh) for keyword in src.keywords]
        if src.latest_rank is not None:
            target.latest_rank = src.latest_rank
        if src.url not in (None, ""):
            target.url = src.url
        if src.mobile_url not in (None, ""):
            target.mobile_url = src.mobile_url
        if src.sentiment_polarity not in (None, ""):
            target.sentiment_polarity = src.sentiment_polarity
        if src.positive_ratio is not None:
            target.positive_ratio = src.positive_ratio
        if src.negative_ratio is not None:
            target.negative_ratio = src.negative_ratio
        if src.neutral_ratio is not None:
            target.neutral_ratio = src.neutral_ratio
        if src.optimism_score is not None:
            target.optimism_score = src.optimism_score
        if src.trust_score is not None:
            target.trust_score = src.trust_score
        if src.controversy_score is not None:
            target.controversy_score = src.controversy_score
        if src.attention_score is not None:
            target.attention_score = src.attention_score
        if src.last_time is not None:
            target.last_time = src.last_time
        if src.analyzed_time is not None:
            target.analyzed_time = src.analyzed_time
        if src.rank_timeline_obj:
            combined_timeline = target.rank_timeline_obj + src.rank_timeline_obj
            dedup_timeline: List[RankTimelineEntry] = []
            seen: Set[Tuple[datetime, int]] = set()
            for point in combined_timeline:
                if point.time is None:
                    continue
                key = (point.time, point.rank)
                if key in seen:
                    continue
                seen.add(key)
                dedup_timeline.append(point)

            target.rank_timeline_obj = sorted(
                dedup_timeline,
                key=lambda x: (x.time, x.rank <= 0, x.rank if x.rank > 0 else 0),
            )
        if src.total_weigh is not None:
            self._set_total_weight(target)
        # 去重 entities 和 keywords
        target.deduplicate_entities_and_keywords()
        return target

    def add_news_items(self, data: NewsData) -> List[NewsItem]:
        news_items = self.expand_news_data_to_items(data)
        key_list = list({(item.source_id, item.title) for item in news_items if item.source_id and item.title})
        if not key_list:
            return []

        for item in news_items:
            self._set_total_weight(item)

        saved = self.storage.add_news_items(news_items)
        return saved

    def add_news_items(self, news_list: List[NewsItem]) -> List[NewsItem]:
        key_list = list({(item.source_id, item.title) for item in news_list if item.source_id and item.title})
        if not key_list:
            return []

        for item in news_list:
            self._set_total_weight(item)

        return self.storage.add_news_items(news_list)

    def expand_news_data_to_items(self, data: NewsData) -> List[NewsItem]:
        news_items: List[NewsItem] = []
        for news_list in data.items.values():
            news_items.extend(news_list)
        return news_items
    
    def group_news_items_by_platform(self, news_items: List[NewsItem]) -> Dict[str, List[NewsItem]]:
        items: Dict[str, List[NewsItem]] = {}
        for item in news_items:
            if item.source_id not in items:
                items[item.source_id] = []
            items[item.source_id].append(item)
        return items

    def get_keywords_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Keyword]:
        """根据起止时间获取关键词列表（直接查询关键词表）。"""
        return self.storage.get_keywords_by_last_time_range(
            start_time=start_time,
            end_time=end_time,
        )

    def get_entities_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> List[Entity]:
        """根据起止时间获取实体列表（直接查询实体表）。"""
        return self.storage.get_entities_by_last_time_range(
            start_time=start_time,
            end_time=end_time,
        )

    def get_news_list_by_keyword_ids(
        self,
        keyword_ids: List[int],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[NewsItem]:
        return self.storage.get_news_list_by_keyword_ids(
            keyword_ids=keyword_ids,
            start_time=start_time,
            end_time=end_time,
        )

    def get_news_list_by_entity_ids(
        self,
        entity_ids: List[int],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[NewsItem]:
        return self.storage.get_news_list_by_entity_ids(
            entity_ids=entity_ids,
            start_time=start_time,
            end_time=end_time,
        )
    
    def update_existing_crawled_titles(self, news_list: List[NewsItem]) -> List[NewsItem]:
        if not news_list:
            return []
    
        key_list = list({(item.source_id, item.title) for item in news_list if item.source_id and item.title})
        if not key_list:
            return []
    
        for item in news_list:
            self._set_total_weight(item)
    
        ok = self.storage.update_crawled_news_list(news_list)
        return ok
    
    def get_news_list_by_source_title_list(self, source_title_list: List[tuple[str, str]]) -> List[NewsItem]:
        return self.storage.get_news_list_by_source_title_list(source_title_list)

    def update_news_list(self, news_list: List[NewsItem]) -> bool:
        if not news_list:
            return False

        key_list = list({(item.source_id, item.title) for item in news_list if item.source_id and item.title})
        if not key_list:
            return False

        existing_items = self.get_news_list_by_source_title_list(key_list)
        existing_map: Dict[Tuple[str, str], NewsItem] = {
            (item.source_id, item.title): item
            for item in existing_items
            if item.source_id and item.title
        }

        merged_items: List[NewsItem] = []
        for incoming in news_list:
            key = (incoming.source_id, incoming.title)
            existing = existing_map.get(key)
            if not existing:
                continue

            merged = self.applyNewsField(incoming, existing)
            self._set_total_weight(merged)
            merged_items.append(merged)

        if not merged_items:
            return False
        return self.storage.update_news_list(merged_items)
    
    def update_news_item(self, item: NewsItem) -> bool:
        if not item.source_id or not item.title:
            return False
        return self.storage.update_news_list([item])

    def get_latest_crawl_data(self, date: Optional[datetime] = None) -> Optional[NewsData]:
        return self.storage.get_latest_crawl_data(date)

    def get_news_list_by_firt_time_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[List[NewsItem]]:
        return self.storage.get_news_list_by_first_time_range(isAnalyzed=isAnalyzed, start_time=start_time, end_time=end_time)

    def get_group_news_by_first_time_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, List[NewsItem]]:
        news_items = self.get_news_list_by_firt_time_range(isAnalyzed=isAnalyzed, start_time=start_time, end_time=end_time)
        result = self._group_items_by_source(news_items) if news_items else {}
        return result

    def get_group_news_by_latest_crawl_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[Dict[str, List[NewsItem]]]:
        news_items = self.get_news_list_by_latest_crawl_range(isAnalyzed=isAnalyzed, start_time=start_time, end_time=end_time)
        if news_items is None:
            return None
        result = self.group_news_items_by_platform(news_items)
        return result
    
    def get_news_list_by_latest_crawl_range(
        self,
        isAnalyzed: bool,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[List[NewsItem]]:
        return self.storage.get_news_list_by_latest_crawl_range(isAnalyzed=isAnalyzed, start_time=start_time, end_time=end_time)

