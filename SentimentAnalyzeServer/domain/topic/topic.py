from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import time
from typing import Any, Dict, List, Optional, Tuple

from SentimentAnalyzeServer.domain.news.news import NewsItem

STAGE_SET = {
	"Inception",
	"Growth",
	"Climax",
	"Decline",
}

def now_timestamp() -> int:
	return int(time.time())

@dataclass(slots=True)
class TopicPlatformStats:
	id: int = field(default=-1)  
	platform: str = ""
	volume: int = 0
	sentiment: str = ""
	ratio: float = 0.0

	def to_dict(self) -> Dict[str, Any]:
		return {
			"platform": self.platform,
			"volume": self.volume,
			"sentiment": self.sentiment,
			"ratio": self.ratio,
		}

	@classmethod
	def from_dict(cls, data: Dict[str, Any]) -> "TopicPlatformStats":
		return cls(
			platform=str(data.get("platform", "")),
			volume=int(data.get("volume", 0) or 0),
			sentiment=str(data.get("sentiment", "")),
			ratio=float(data.get("ratio", 0.0) or 0.0),
		)

@dataclass(slots=True)
class Topic: 
	created_at: Optional[int] = None #不是now，而是第一次构建Topic的时间戳
	id: int = field(default=-1)  
	topic: str = ""
	llm_title: Optional[str] = None
	topic_type: Optional[str] = None
	rank_data: Dict[str, List[NewsItem]] = field(default_factory=dict)
	platform_distribution: List[TopicPlatformStats] = field(default_factory=list)
	start_time: Optional[int] = None #时间窗口的开始时间戳
	end_time: Optional[int] = None #时间窗口的结束时间戳
	window_size: int = 0  #单位分钟
	sentiment: str = ""
	news_count: int = 0
	updated_at: Optional[int] = None
	version: int = 0

	total_weight: float = 0.0 #等同于热度
	heat_change_percent: float = 0.0
	stage: str = ""

	@property
	def source_diversity(self) -> int:
		return len(self.platform_distribution)*3
	
	@staticmethod
	def build_rank_key(item: NewsItem) -> str:
		return f"{item.source_id}::{item.id}::{item.title}"

	def to_dict(self) -> Dict[str, Any]:
		return {
			"id": self.id,
			"topic": self.topic,
			"llm_title": self.llm_title,
			"topic_type": self.topic_type,
			"rank_data": {
				key: [item.to_dict() for item in items]
				for key, items in self.rank_data.items()
			},
			"platform_distribution": [item.to_dict() for item in self.platform_distribution],
			"start_time": self.start_time,
			"end_time": self.end_time,
			"window_size": self.window_size,
			"sentiment": self.sentiment,
			"news_count": self.news_count,
			"total_weight": self.total_weight,
			"heat_change_percent": self.heat_change_percent,
			"stage": self.stage,
			"source_diversity": self.source_diversity,
			"created_at": self.created_at,
			"updated_at": self.updated_at,
			"version": self.version,
		}

	@classmethod
	def from_dict(cls, data: Dict[str, Any]) -> "Topic":
		def _to_optional_int_timestamp(value: Any) -> Optional[int]:
			if value is None:
				return None
			if isinstance(value, int):
				return int(value)
			raise TypeError(f"timestamp must be int, got {type(value).__name__}")

		raw_rank_data = data.get("rank_data", {})
		rank_data: Dict[str, List[NewsItem]] = {}

		if isinstance(raw_rank_data, dict):
			for key, items in raw_rank_data.items():
				if isinstance(items, list):
					rank_data[str(key)] = [
						NewsItem.from_dict(item) if isinstance(item, dict) else item
						for item in items
					]
				elif isinstance(items, dict):
					rank_data[str(key)] = [NewsItem.from_dict(items)]

		raw_distribution = data.get("platform_distribution", [])
		platform_distribution: List[TopicPlatformStats] = []

		if isinstance(raw_distribution, list):
			platform_distribution = [
				TopicPlatformStats.from_dict(item)
				for item in raw_distribution
				if isinstance(item, dict)
			]
		elif isinstance(raw_distribution, dict):
			# Backward compatible with old dict format
			platform_distribution = [
				TopicPlatformStats.from_dict(item)
				for item in raw_distribution.values()
				if isinstance(item, dict)
			]
		return cls(
			id=int(data.get("id", -1) or -1),
			topic=str(data.get("topic", "")),
			llm_title=(None if data.get("llm_title") is None else str(data.get("llm_title"))),
			topic_type=(None if data.get("topic_type") is None else str(data.get("topic_type"))),
			rank_data=rank_data,
			platform_distribution=platform_distribution,
			start_time=_to_optional_int_timestamp(data.get("start_time")),
			end_time=_to_optional_int_timestamp(data.get("end_time")),
			window_size=int(data.get("window_size", 0) or 0),
			sentiment=str(data.get("sentiment", "") or ""),
			news_count=int(data.get("news_count", 0) or 0),
			total_weight=float(data.get("total_weight", 0.0) or 0.0),
			heat_change_percent=float(data.get("heat_change_percent", 0.0) or 0.0),
			stage=str(data.get("stage", "") or ""),
			created_at=_to_optional_int_timestamp(data.get("created_at")),
			updated_at=_to_optional_int_timestamp(data.get("updated_at")),
			version=int(data.get("version", 0) or 0),
		)

class TopicDomainService:
	def __init__(
		self,
		topic_repository: Optional["TopicRepository"] = None,
		heat_stage_config: Optional[Dict[str, Any]] = None,
	) -> None:
		self.topic_repository = topic_repository
		config = heat_stage_config or {}
		self.decline_threshold_percent = self._to_float(config.get("decline_threshold_percent"), -15.0)
		self.climax_peak_ratio = self._to_float(config.get("climax_peak_ratio"), 0.9)
		self.climax_change_abs_limit_percent = self._to_float(config.get("climax_change_abs_limit_percent"), 20.0)
		self.growth_threshold_percent = self._to_float(config.get("growth_threshold_percent"), 20.0)

	@staticmethod
	def _to_float(value: Any, default: float) -> float:
		try:
			return float(value)
		except (TypeError, ValueError):
			return default

	def calculate_heat_change_and_stage(self, topic: Topic, history_snapshots: List[Topic]) -> Topic:
		"""根据历史快照计算热度变化百分比与阶段。"""
		current_heat = float(topic.total_weight or 0.0)
		history = [
			s for s in history_snapshots
			if isinstance(s, Topic)
		]
		history.sort(
			key=lambda s: (int(s.updated_at or 0), int(s.created_at or 0), int(s.id or -1)),
			reverse=True,
		)

		prev_heat = float(history[0].total_weight or 0.0) if history else 0.0
		if prev_heat > 0:
			topic.heat_change_percent = ((current_heat - prev_heat) / prev_heat) * 100.0
		elif current_heat > 0:
			topic.heat_change_percent = 100.0
		else:
			topic.heat_change_percent = 0.0

		heat_series = [float(s.total_weight or 0.0) for s in history]
		heat_series.append(current_heat)
		peak_heat = max(heat_series) if heat_series else current_heat
		change = float(topic.heat_change_percent or 0.0)

		if not history:
			topic.stage = "Inception"
		elif change <= self.decline_threshold_percent:
			topic.stage = "Decline"
		elif (
			peak_heat > 0
			and current_heat >= peak_heat * self.climax_peak_ratio
			and abs(change) < self.climax_change_abs_limit_percent
		):
			topic.stage = "Climax"
		elif change >= self.growth_threshold_percent:
			topic.stage = "Growth"
		elif current_heat >= prev_heat:
			topic.stage = "Growth"
		else:
			topic.stage = "Decline"

		if topic.stage not in STAGE_SET:
			topic.stage = "Inception"
		return topic

	def enrich_topic_with_history(self, topic: Topic, history_limit: int = 12) -> Topic:
		if self.topic_repository is None:
			if not topic.stage:
				topic.stage = "Inception"
			topic.heat_change_percent = float(topic.heat_change_percent or 0.0)
			return topic

		history = self.topic_repository.list_topic_history(
			topic_name=topic.topic,
			limit=max(1, int(history_limit)),
			end_time=topic.updated_at,
		)
		return self.calculate_heat_change_and_stage(topic, history)

	def save_topic_snapshot(self, topic: Topic) -> Topic:
		if self.topic_repository is None:
			return topic
		return self.topic_repository.save_topic_snapshot(topic)

	def save_or_update_topic_snapshot(
		self,
		topic: Topic,
		days_lookback: int = 7,
	) -> Topic:
		"""
		保存或更新Topic快照。
		
		- 如果存在相同topic名称且updated_at在最近days_lookback天内的记录，
		  则执行UPDATE操作（保持主键不变）并将旧数据加入历史。
		- 否则执行INSERT操作创建新记录。
		
		Args:
			topic: 新的Topic数据
			days_lookback: 向后查看的天数（默认7天）
		
		Returns:
			保存或更新后的Topic对象
		"""
		if self.topic_repository is None:
			return topic
		
		topic_name = str(topic.topic or "").strip()
		if not topic_name:
			return self.topic_repository.save_topic_snapshot(topic)
		
		# 查找最近7天内相同topic的最新记录
		recent_topic = self.topic_repository.find_recent_topic_by_name(
			topic_name=topic_name,
			days_lookback=days_lookback,
		)
		
		if recent_topic is not None and int(recent_topic.id or -1) > 0:
			# 存在最近的相同topic记录，执行UPDATE操作
			return self.topic_repository.update_topic_snapshot(
				topic=topic,
				existing_created_at=int(recent_topic.created_at or int(time.time())),
				existing_id=int(recent_topic.id),
			)
		else:
			# 不存在相同的最近记录，执行INSERT操作
			return self.topic_repository.save_topic_snapshot(topic)

	def append_topic_metrics_history_async_safe(self, topic: Topic, snapshot_time: Optional[int] = None) -> None:
		if self.topic_repository is None:
			return
		self.topic_repository.append_topic_metrics_history(topic, snapshot_time=snapshot_time)

	def list_topics_missing_llm_title(self, limit: int = 50) -> List[Topic]:
		if self.topic_repository is None:
			return []
		return self.topic_repository.list_topics_missing_llm_title(limit=max(1, int(limit)))

	def update_topic_llm_title_only(
		self,
		topic_created_at: int,
		topic_id: int,
		llm_title: str,
	) -> bool:
		if self.topic_repository is None:
			return False
		return self.topic_repository.update_topic_llm_title_only(
			topic_created_at=int(topic_created_at),
			topic_id=int(topic_id),
			llm_title=str(llm_title or "").strip(),
		)

	def get_topic_timeline_and_latest(
		self,
		topic_created_at: int,
		topic_id: int,
		history_limit: int = 100,
	) -> List[Topic]:
		if self.topic_repository is None:
			return []

		base_topic = self.topic_repository.get_topic_by_composite_key(topic_created_at, topic_id)
		if base_topic is None:
			return []

		history = self.topic_repository.list_topic_metrics_history_by_composite_key(
			topic_created_at=topic_created_at,
			topic_id=topic_id,
			limit=max(1, int(history_limit)),
		)

		latest_topic = self.topic_repository.get_latest_topic_snapshot(base_topic.topic)

		combined: List[Topic] = []
		seen_keys: set[tuple[str, int, str]] = set()

		for item in [base_topic, latest_topic, *history]:
			if item is None:
				continue
			updated_at_key = str(int(item.updated_at or 0))
			key = (str(int(item.created_at or 0)), int(item.id or -1), updated_at_key)
			if key in seen_keys:
				continue
			seen_keys.add(key)
			combined.append(item)

		combined.sort(
			key=lambda x: (int(x.updated_at or 0), int(x.created_at or 0), int(x.id or -1)),
			reverse=True,
		)
		return combined
	
	def aggregate_topic_metrics(self, topic: Topic) -> Topic:
        
		"""
		计算平台分布信息
		- volume: 同一平台所有 NewsItem 的 total_weigh 之和
		- sentiment: 该平台该 TOPIC 下 total_weigh 求和最大的情感极性
		"""
		# 临时数据结构用于计算
		platform_data: Dict[str, Dict[str, float]] = {}  # {platform: {sentiment: sum_weight}}
		platform_volumes: Dict[str, float] = {}  # {platform: total_weight}
		sentiment_totals: Dict[str, float] = {}
		news_count = 0
		total_weight = 0.0
		start_time: Optional[int] = None
		end_time: Optional[int] = None
		
		# 单次遍历所有 NewsItem，完成 topic 层和平台层所需的基础统计。
		for _, news_items in topic.rank_data.items():
			if not isinstance(news_items, list):
				continue
			
			for item in news_items:
				platform = (item.source_id or "").strip() or "unknown"
				news_count += 1
				total_weight += item.total_weigh
				if item.first_time and (start_time is None or item.first_time < start_time):
					start_time = item.first_time
				if item.last_time and (end_time is None or item.last_time > end_time):
					end_time = item.last_time
				
				# 初始化平台数据
				if platform not in platform_data:
					platform_data[platform] = {}
					platform_volumes[platform] = 0.0
				
				# 累加权重到 volume
				platform_volumes[platform] += item.total_weigh
				
				# 按情感极性累加权重
				sentiment = item.sentiment_polarity or "neutral"
				if sentiment not in platform_data[platform]:
					platform_data[platform][sentiment] = 0.0
				platform_data[platform][sentiment] += item.total_weigh
				sentiment_totals[sentiment] = sentiment_totals.get(sentiment, 0.0) + item.total_weigh
		
		# 构建 platform_distribution，并同时汇总 topic 情感评分。
		result_distribution: List[TopicPlatformStats] = []
		topic_sentiment_scores: Dict[str, float] = {}
		
		for platform, sentiments in platform_data.items():
			# 找出该平台权重最大的情感极性
			max_sentiment = max(sentiments.items(), key=lambda x: x[1])[0] if sentiments else "neutral"
			max_weight = sentiments.get(max_sentiment, 0.0)
			
			# 计算该平台在所有情感中的占比
			platform_total_weight = platform_volumes[platform]
			ratio = max_weight / platform_total_weight if platform_total_weight > 0 else 0.0
			
			topic_platform_stats = TopicPlatformStats(
				platform=platform,
				volume=int(platform_volumes[platform]),
				sentiment=max_sentiment,
				ratio=ratio
			)
			result_distribution.append(topic_platform_stats)

			sentiment_key = (max_sentiment or "").strip() or "neutral"
			topic_sentiment_scores[sentiment_key] = topic_sentiment_scores.get(sentiment_key, 0.0) + (
				float(platform_volumes[platform]) * ratio
			)
		
		# 按 volume 排序，大的放前面
		result_distribution.sort(key=lambda x: x.volume, reverse=True)

		# 更新 topic 的 platform_distribution
		topic.platform_distribution = result_distribution
		topic.start_time = start_time
		topic.end_time = end_time
		if start_time and end_time:
			topic.window_size = max(0, int((int(end_time) - int(start_time)) // 60)) # 单位分钟
		else:
			topic.window_size = 0

		if topic_sentiment_scores:
			topic.sentiment = max(topic_sentiment_scores.items(), key=lambda x: x[1])[0]
		elif sentiment_totals:
			# 兼容兜底：若平台分布为空，则回退到 item 级别累计权重。
			topic.sentiment = max(sentiment_totals.items(), key=lambda x: x[1])[0]
		else:
			topic.sentiment = ""
		topic.news_count = news_count
		topic.total_weight = total_weight
		topic.total_weight+=topic.source_diversity #考虑增加平台多样性对热度的贡献
		now = now_timestamp()
		if not topic.created_at:
			topic.created_at = now
		topic.updated_at = now
		topic.version += 1
		return topic

	def build_topic_from_news_items(self, topic_name: str, news_items: List[NewsItem]) -> Topic:
		topic = Topic(topic=topic_name)
		for item in news_items:
			if not isinstance(item, NewsItem):
				continue
			source_key = (item.source_id or "").strip() or "unknown"
			topic.rank_data.setdefault(source_key, []).append(item)
			topic.total_weight += item.total_weigh
			topic.news_count += 1
			if item.first_time and (topic.start_time is None or item.first_time < topic.start_time):
				topic.start_time = item.first_time
			if item.last_time and (topic.end_time is None or item.last_time > topic.end_time):
				topic.end_time = item.last_time

		if topic.start_time and topic.end_time:
			topic.window_size = max(0, int((int(topic.end_time) - int(topic.start_time)) // 60))
		now = now_timestamp()
		if not topic.created_at:
			topic.created_at = now
		topic.updated_at = now
		return self.aggregate_topic_metrics(topic)

class TopicRepository(ABC):
	@abstractmethod
	def save_topic_snapshot(self, topic: Topic) -> Topic:
		pass

	@abstractmethod
	def get_latest_topic_snapshot(self, topic_name: str) -> Optional[Topic]:
		pass

	@abstractmethod
	def list_topic_history(
		self,
		topic_name: str,
		limit: int = 30,
		end_time: Optional[int] = None,
	) -> List[Topic]:
		pass

	@abstractmethod
	def get_topic_by_composite_key(self, topic_created_at: int, topic_id: int) -> Optional[Topic]:
		pass

	@abstractmethod
	def append_topic_metrics_history(self, topic: Topic, snapshot_time: Optional[int] = None) -> None:
		pass

	@abstractmethod
	def list_topic_metrics_history_by_composite_key(
		self,
		topic_created_at: int,
		topic_id: int,
		limit: int = 100,
	) -> List[Topic]:
		pass

	@abstractmethod
	def find_recent_topic_by_name(
		self,
		topic_name: str,
		days_lookback: int = 7,
	) -> Optional[Topic]:
		"""查找最近N天内相同topic名称的最新记录."""
		pass

	@abstractmethod
	def update_topic_snapshot(
		self,
		topic: Topic,
		existing_created_at: int,
		existing_id: int,
	) -> Topic:
		"""更新现有Topic快照，保持主键不变，并将旧数据加入历史."""
		pass

	@abstractmethod
	def list_topics_missing_llm_title(self, limit: int = 50) -> List[Topic]:
		"""列出 llm_title 为空的Topic快照。"""
		pass

	@abstractmethod
	def update_topic_llm_title_only(
		self,
		topic_created_at: int,
		topic_id: int,
		llm_title: str,
	) -> bool:
		"""仅更新 llm_title 字段，不修改其他字段（包括 updated_at/version）。"""
		pass
    
