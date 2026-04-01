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
	"Maturity",
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
	# 主键
	created_at: Optional[int] = None #不是now，而是第一次构建Topic的时间戳
	id: int = field(default=-1)  
	# 需要保持的字段
	topic: str = ""
	llm_title: Optional[str] = None
	topic_type: Optional[str] = None
	# 新状态会有的字段
	rank_data: Dict[str, List[NewsItem]] = field(default_factory=dict)
	platform_distribution: List[TopicPlatformStats] = field(default_factory=list)
	start_time: Optional[int] = None #时间窗口的开始时间戳
	end_time: Optional[int] = None #时间窗口的结束时间戳
	window_size: int = 0  #单位分钟
	sentiment: str = ""
	news_count: int = 0
	updated_at: Optional[int] = None
	total_weight: float = 0.0
	# 需要计算的字段
	version: int = 0
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

	def add_topics(self, topics: List["Topic"]) -> List["Topic"]:
			if self.topic_repository is None:
				return topics
			persisted = self.topic_repository.add_topics(topics)
			self.append_topics_metrics_histories(persisted)
			return persisted

	def append_topics_metrics_histories(self, topics: List["Topic"], snapshot_time: Optional[int] = None) -> None:
			if self.topic_repository is None:
				return
			self.topic_repository.append_topic_metrics_histories(topics)

	def update_topics(self, topics: List["Topic"]) -> List["Topic"]:
			if self.topic_repository is None:
				return topics
			updated = self.topic_repository.update_topics(topics)
			self.append_topics_metrics_histories(updated)
			return updated

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

	def list_topics_by_time_range(
            self,
            created_at_start: Optional[int] = None,
            created_at_end: Optional[int] = None,
            updated_at_start: Optional[int] = None,
            updated_at_end: Optional[int] = None,
            limit: int = 100,
    ) -> List[Topic]:
		if self.topic_repository is None:
			return []
		return self.topic_repository.list_topics_by_time_range(
			created_at_start=created_at_start,
			created_at_end=created_at_end,
			updated_at_start=updated_at_start,
			updated_at_end=updated_at_end,
			limit=limit,
		)

	@staticmethod
	def _to_float(value: Any, default: float) -> float:
		try:
			return float(value)
		except (TypeError, ValueError):
			return default

	def calculate_heat_change_and_stage(self, topic: Topic, history_snapshots: List[Topic]) -> Topic:
		"""
		重新设计的话题生命周期算法：基于多点平滑与动态阈值
		"""
		current_heat = float(topic.total_weight or 0.0)

			# 1. 历史数据预处理 (确保按时间升序，方便计算趋势)
		history = sorted(
			[s for s in history_snapshots if isinstance(s, Topic)],
			key=lambda s: (int(s.updated_at or 0), int(s.id or -1))
		)
		# 获取历史序列（包含当前值）
		heat_series = [float(s.total_weight or 0.0) for s in history] + [current_heat]
		# 2. 计算核心指标
		prev_heat = heat_series[-2] if len(heat_series) > 1 else 0.0
		# 计算瞬时变化率
		change = ((current_heat - prev_heat) / prev_heat * 100.0) if prev_heat > 0 else 0.0
		topic.heat_change_percent = change
		# 计算移动平均 (最近3个点) 减少噪音
		window_size = 3
		recent_avg = sum(heat_series[-window_size:]) / len(heat_series[-window_size:])
		historical_peak = max(heat_series) if heat_series else current_heat
		# 3. 阶段判定逻辑 (状态机模式)
		# A. 初始阶段
		if len(history) < 2:
			topic.stage = "Inception"
		# B. 衰退阶段 (跌幅超过阈值 或 远低于峰值)
		elif change <= self.decline_threshold_percent or current_heat < historical_peak * 0.3:
			topic.stage = "Decline"
		# C. 鼎盛阶段 (处于高位平台期：热度接近峰值 且 变化平缓)
		elif (
			current_heat >= historical_peak * self.climax_peak_ratio
			and abs(change) < self.climax_change_abs_limit_percent
		):
			topic.stage = "Climax"

		# D. 爆发增长阶段 (瞬时涨幅大 或 持续走高)
		elif change >= self.growth_threshold_percent:
			topic.stage = "Growth"
		# E. 成熟阶段 (高位小幅下滑或回落)
		elif current_heat > historical_peak * 0.6 and change < 0:
			topic.stage = "Maturity"
		# F. 兜底逻辑
		else:
			topic.stage = "Growth" if current_heat >= prev_heat else "Decline"
		# 4. 安全校验
		if topic.stage not in STAGE_SET:
			topic.stage = "Inception"
		return topic
	
	def applyNewStatus(self, new_status: Topic, old_status: Topic) -> Topic:
		"""
		应用新状态，但不计算热度变化百分比和阶段
		"""
		# 主键
		new_status.created_at = old_status.created_at
		new_status.id = old_status.id

		new_status.topic = old_status.topic
		new_status.llm_title = old_status.llm_title
		new_status.topic_type = old_status.topic_type

		new_status.version = old_status.version + 1

		return new_status

	def get_topic_history(self, topic: Topic, history_limit: int = 1000) -> List[Topic]:
		history = self.topic_repository.list_topic_metrics_history_by_composite_key(
			topic_created_at=topic.created_at,
			topic_id=topic.id,
			limit=max(1, int(history_limit)),
		)
		if not history:
			return []
		return history

	def add_topic(self, topic: Topic) -> Topic:
		"""添加新Topic。同时添加第一条记录"""
		if self.topic_repository is None:
			return topic
		pesisted_topic = self.topic_repository.add_topics([topic])
		self.append_topics_metrics_histories(pesisted_topic)
		return pesisted_topic

	def append_topic_metrics_history(self, topic: Topic, snapshot_time: Optional[int] = None) -> None:
		if self.topic_repository is None:
			return
		self.topic_repository.append_topic_metrics_histories([topic], snapshot_time=snapshot_time)

	def list_topics_missing_llm_title(self, limit: int = 50) -> List[Topic]:
		if self.topic_repository is None:
			return []
		return self.topic_repository.list_topics_missing_llm_title(limit=max(1, int(limit)))

	def should_summarize_llm_title(self, topic: Topic) -> bool:
		"""判断 Topic 是否需要进行 llm_title 总结。"""
		if not isinstance(topic, Topic):
			return False
		
		has_enough_news = topic.news_count >= 3
		has_high_weight = topic.total_weight >= 90.0
		must = has_enough_news and has_high_weight	

		return must

	def update_topic(self, topic: Topic) -> Optional[Topic]:
		if self.topic_repository is None:
			return None
		if topic.created_at is None or topic.id is None:
			raise ValueError("Topic must have created_at and id for update.")
		updated = self.topic_repository.update_topics(
			topics=[topic],
		)
		self.append_topics_metrics_histories(updated)
		return updated

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

		latest_topic = self.topic_repository.get_topic_by_composite_key(topic_created_at, topic_id)
		if latest_topic is None:
			return []

		history = self.topic_repository.list_topic_metrics_history_by_composite_key(
			topic_created_at=topic_created_at,
			topic_id=topic_id,
			limit=max(1, int(history_limit)),
		)
		combined: List[Topic] = []
		seen_keys: set[tuple[str, int, str]] = set()
		combined.append(latest_topic)
		for item in [*history]:
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

	def find_recent_topic_by_name(self, topic_name: str, days_lookback: int = 7) -> Optional[Topic]:
		if self.topic_repository is None:
			return None
		return self.topic_repository.find_recent_topic_by_name(
			topic_name=str(topic_name),
			days_lookback=max(1, int(days_lookback)),
		)

class TopicRepository(ABC):

	@abstractmethod
	def add_topics(self, topics: List["Topic"]) -> List["Topic"]:
		"""批量插入Topic，返回带主键的Topic列表。默认实现为循环调用add_topic。"""
		pass

	@abstractmethod
	def append_topic_metrics_histories(self, topics: List["Topic"], snapshot_time: Optional[int] = None) -> None:
		"""批量插入Topic历史。"""
		pass

	@abstractmethod
	def update_topics(self, topics: List["Topic"]) -> List["Topic"]:
		"""批量更新Topic，不更新llm_title，返回更新后的Topic列表。默认实现为循环调用update_topic。"""
		pass
	
	@abstractmethod
	def list_topics_by_time_range(
		self,
		first_time_start: Optional[int] = None,
		first_time_end: Optional[int] = None,
		updated_at_start: Optional[int] = None,
		updated_at_end: Optional[int] = None,
		limit: int = 100,
	) -> List[Topic]:
		"""根据first_time和updated_at的起止时间，返回Topic表中的所有Topic。"""
		pass

	@abstractmethod
	def get_topic_by_composite_key(self, topic_created_at: int, topic_id: int) -> Optional[Topic]:
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
    

