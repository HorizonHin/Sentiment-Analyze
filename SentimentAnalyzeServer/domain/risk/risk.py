from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import time
from typing import Any, Dict, List, Optional, Set

from SentimentAnalyzeServer.domain.topic.topic import Topic, TopicPlatformStats

RISK_TYPE_NEGATIVE_CLUSTER = "negative_cluster"
RISK_TYPE_BURST_EVENT = "burst_event"
RISK_TYPE_CROSS_PLATFORM_GAP = "cross_platform_gap"
RISK_TYPE_SENSITIVE_KEYWORDS = "sensitive_keywords"

RISK_LEVEL_LOW = "low"
RISK_LEVEL_MEDIUM = "medium"
RISK_LEVEL_HIGH = "high"
RISK_LEVEL_CRITICAL = "critical"


def now_timestamp() -> int:
    return int(time.time())


@dataclass(slots=True)
class TopicRiskWarning:
    topic_created_at: int
    topic_id: int
    topic_name: str = ""
    risk_type: str = ""
    risk_level: str = RISK_LEVEL_LOW
    risk_score: int = 0
    reason: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)
    detected_by_event: str = ""
    occurred_at: int = field(default_factory=now_timestamp)


@dataclass(slots=True)
class SensitiveTitleRecord:
    topic_created_at: int
    topic_id: int
    topic_name: str = ""
    old_topic: str = ""
    candidate_titles: List[str] = field(default_factory=list)
    reason: str = ""
    risk_level: str = RISK_LEVEL_HIGH
    occurred_at: int = field(default_factory=now_timestamp)
    context: Dict[str, Any] = field(default_factory=dict)


class RiskWarningRepository(ABC):
    @abstractmethod
    def add_topic_risk_warnings(self, warnings: List[TopicRiskWarning]) -> int:
        pass

    @abstractmethod
    def add_sensitive_title_records(self, records: List[SensitiveTitleRecord]) -> int:
        pass

    @abstractmethod
    def get_topic_risk_warnings(
        self,
        topic_created_at: Optional[int] = None,
        topic_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        risk_level: Optional[str] = None,
        limit: int = 100,
    ) -> List[TopicRiskWarning]:
        pass

    @abstractmethod
    def get_sensitive_title_records(
        self,
        topic_created_at: Optional[int] = None,
        topic_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[SensitiveTitleRecord]:
        pass


class RiskWarningDomainService:
    def __init__(
        self,
        risk_warning_repository: RiskWarningRepository,
        risk_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.risk_warning_repository = risk_warning_repository
        config = risk_config or {}

        self.negative_ratio_threshold = self._to_float(config.get("negative_ratio_threshold"), 0.60)
        self.negative_news_count_threshold = self._to_int(config.get("negative_news_count_threshold"), 20)
        self.burst_heat_change_threshold = self._to_float(config.get("burst_heat_change_threshold"), 60.0)
        self.cross_platform_gap_threshold = self._to_float(config.get("cross_platform_gap_threshold"), 0.35)
        
        # 敏感词配置
        self.sensitive_keywords = {
            "爆炸", "起火", "泄露", "中毒", # 安全类
            "诉讼", "投诉", "违规", "受罚", # 法律类
            "欺诈", "骚扰", "歧视", # 道德类
            "罢工", "抵制", "联署"  # 舆论类
        }
        self.sensitive_keyword_count_threshold = self._to_int(config.get("sensitive_keyword_count_threshold"), 5)

    @staticmethod
    def _to_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _score_to_level(score: int) -> str:
        score = max(0, min(100, int(score)))
        if score >= 75:
            return RISK_LEVEL_CRITICAL
        if score >= 50:
            return RISK_LEVEL_HIGH
        if score >= 25:
            return RISK_LEVEL_MEDIUM
        return RISK_LEVEL_LOW

    @staticmethod
    def _safe_topic_keys(topic: Topic) -> tuple[int, int]:
        return (int(getattr(topic, "created_at", 0) or 0), int(getattr(topic, "id", -1) or -1))

    @staticmethod
    def _platform_sentiment_score(platform_stat: TopicPlatformStats) -> float:
        sentiment = str(platform_stat.sentiment or "").strip().lower()        
        if sentiment in {"unknown", "neutral", "","mixed"}:
            return 0.0        
        ratio = float(platform_stat.ratio or 0.0)
        if sentiment == "positive":
            base = 1.0
        elif sentiment == "negative":
            base = -1.0
        else:
            base = 0.0
        return base * max(0.0, min(1.0, ratio))

    @staticmethod
    def _iter_news_items(topic: Topic):
        for items in (topic.rank_data or {}).values():
            if not isinstance(items, list):
                continue
            for item in items:
                yield item

    def _compute_negative_ratio(self, topic: Topic) -> float:
        total_weight = 0.0
        negative_weight = 0.0
        for item in self._iter_news_items(topic):
            item_weight = float(getattr(item, "total_weigh", 0.0) or 0.0)
            if item_weight <= 0:
                item_weight = 1.0
            total_weight += item_weight
            polarity = str(getattr(item, "sentiment_polarity", "") or "").strip().lower()
            if polarity == "negative":
                negative_weight += item_weight

        if total_weight <= 0:
            return 0.0
        return max(0.0, min(1.0, negative_weight / total_weight))

    def _check_negative_cluster(self, topic: Topic, occurred_at: int, detected_by_event: str) -> Optional[TopicRiskWarning]:
        negative_ratio = self._compute_negative_ratio(topic)
        news_count = int(getattr(topic, "news_count", 0) or 0)
        if negative_ratio < self.negative_ratio_threshold or news_count < self.negative_news_count_threshold:
            return None

        score = int(min(100, 55 + max(0.0, (negative_ratio - self.negative_ratio_threshold) * 100.0) + min(20.0, news_count / 5.0)))
        topic_created_at, topic_id = self._safe_topic_keys(topic)
        return TopicRiskWarning(
            topic_created_at=topic_created_at,
            topic_id=topic_id,
            topic_name=str(getattr(topic, "topic", "") or ""),
            risk_type=RISK_TYPE_NEGATIVE_CLUSTER,
            risk_level=self._score_to_level(score),
            risk_score=score,
            reason=(
                f"负面舆情占比过高: {negative_ratio:.1%} (阈值: {self.negative_ratio_threshold:.0%})，"
                f"且负面新闻数量达到 {news_count} 篇 (起征点: {self.negative_news_count_threshold})"
            ),
            metrics={
                "negative_ratio": negative_ratio,
                "news_count": news_count,
                "thresholds": {
                    "negative_ratio": self.negative_ratio_threshold,
                    "news_count": self.negative_news_count_threshold,
                },
            },
            detected_by_event=detected_by_event,
            occurred_at=occurred_at,
        )

    def _check_burst_event(self, topic: Topic, occurred_at: int, detected_by_event: str) -> Optional[TopicRiskWarning]:
        heat_change_percent = float(getattr(topic, "heat_change_percent", 0.0) or 0.0)
        stage = str(getattr(topic, "stage", "") or "")
        if heat_change_percent < self.burst_heat_change_threshold or stage not in {"Growth", "Climax"}:
            return None

        score = int(min(100, 50 + (heat_change_percent - self.burst_heat_change_threshold) * 0.8 + (10 if stage == "Climax" else 0)))
        topic_created_at, topic_id = self._safe_topic_keys(topic)
        
        stage_map = {"Growth": "爆发增长期", "Climax": "高潮平稳期"}
        cn_stage = stage_map.get(stage, stage)

        return TopicRiskWarning(
            topic_created_at=topic_created_at,
            topic_id=topic_id,
            topic_name=str(getattr(topic, "topic", "") or ""),
            risk_type=RISK_TYPE_BURST_EVENT,
            risk_level=self._score_to_level(score),
            risk_score=score,
            reason=(
                f"话题热度异常爆发: 增长率 {heat_change_percent:.1f}% (阈值: {self.burst_heat_change_threshold:.1f}%)，"
                f"当前处于话题周期的 {cn_stage}"
            ),
            metrics={
                "heat_change_percent": heat_change_percent,
                "stage": stage,
                "thresholds": {"heat_change_percent": self.burst_heat_change_threshold},
            },
            detected_by_event=detected_by_event,
            occurred_at=occurred_at,
        )

    def _check_cross_platform_gap(self, topic: Topic, occurred_at: int, detected_by_event: str) -> Optional[TopicRiskWarning]:
        stats = [item for item in (topic.platform_distribution or []) if isinstance(item, TopicPlatformStats) and int(item.volume or 0) > 0]
        if len(stats) < 2:
            return None

        major_stats = sorted(stats, key=lambda x: int(x.volume or 0), reverse=True)[:3]
        platform_scores = []
        for item in major_stats:
            platform_scores.append(
                {
                    "platform": str(item.platform or ""),
                    "sentiment": str(item.sentiment or ""),
                    "ratio": float(item.ratio or 0.0),
                    "volume": int(item.volume or 0),
                    "score": self._platform_sentiment_score(item),
                }
            )

        if len(platform_scores) < 2:
            return None

        score_values = [float(item["score"]) for item in platform_scores]
        # 过滤掉分值为 0 的项（对应 sentiment 为 unknown 的平台）
        valid_scores = [v for v in score_values if v != 0.0]
        if len(valid_scores) < 2:
            return None

        max_score = max(valid_scores)
        min_score = min(valid_scores)
        gap = max_score - min_score
        has_positive = any(value > 0.2 for value in valid_scores)
        has_negative = any(value < -0.2 for value in valid_scores)
        polarity_conflict = has_positive and has_negative

        if gap < self.cross_platform_gap_threshold or not polarity_conflict:
            return None

        score = int(min(100, 45 + (gap - self.cross_platform_gap_threshold) * 100 + 10))
        topic_created_at, topic_id = self._safe_topic_keys(topic)
        return TopicRiskWarning(
            topic_created_at=topic_created_at,
            topic_id=topic_id,
            topic_name=str(getattr(topic, "topic", "") or ""),
            risk_type=RISK_TYPE_CROSS_PLATFORM_GAP,
            risk_level=self._score_to_level(score),
            risk_score=score,
            reason=(
                f"跨平台舆情存在巨大差异: 情感分差值 {gap:.3f} (阈值: {self.cross_platform_gap_threshold:.2f})，"
                f"且各平台之间存在明显的极性对立 (正负并存)"
            ),
            metrics={
                "platform_sentiment_gap": gap,
                "polarity_conflict": polarity_conflict,
                "platform_scores": platform_scores,
                "thresholds": {"cross_platform_gap": self.cross_platform_gap_threshold},
            },
            detected_by_event=detected_by_event,
            occurred_at=occurred_at,
        )

    def _check_sensitive_keywords(self, topic: Topic, occurred_at: int, detected_by_event: str) -> Optional[TopicRiskWarning]:
        """
        检测是否有敏感词触发风险
        IF 敏感词出现 AND 新闻>5条 THEN risk_level = HIGH
        """
        news_count = int(getattr(topic, "news_count", 0) or 0)
        # 如果新闻条数少于阈值，则不触发
        if news_count < self.sensitive_keyword_count_threshold:
            return None

        found_keywords: Set[str] = set()
        
        # 遍历所有新闻
        for item in self._iter_news_items(topic):
            # 1. 从 keywords 查找 (NewsKeyword 对象在 items.keywords 属性里)
            if hasattr(item, "keywords"):
                for kw in item.keywords:
                    # kw 可能是 NewsKeyword 数据类，也可能是 dict（如果存储层之前反序列化）
                    term = getattr(kw, "term", "") if not isinstance(kw, dict) else kw.get("term", "")
                    if term in self.sensitive_keywords:
                        found_keywords.add(str(term))
            
            # 2. 从 summary 查找
            summary = str(getattr(item, "summary", "") or "")
            if summary:
                for target_kw in self.sensitive_keywords:
                    if target_kw in summary:
                        found_keywords.add(target_kw)

        if not found_keywords:
            return None

        topic_created_at, topic_id = self._safe_topic_keys(topic)
        return TopicRiskWarning(
            topic_created_at=topic_created_at,
            topic_id=topic_id,
            topic_name=str(getattr(topic, "topic", "") or ""),
            risk_type=RISK_TYPE_SENSITIVE_KEYWORDS,
            risk_level=RISK_LEVEL_HIGH, # 触发规则定为 HIGH
            risk_score=70, # 配合 HIGH 等级（>=50 且 <75 为 HIGH）
            reason=(
                f"触发敏感关键词检测: {', '.join(sorted(list(found_keywords)))}。 "
                f"当前话题新闻数量已达 {news_count} 篇 (起征点: {self.sensitive_keyword_count_threshold})。"
            ),
            metrics={
                "found_keywords": list(found_keywords),
                "news_count": news_count,
                "thresholds": {"sensitive_keyword_count": self.sensitive_keyword_count_threshold},
            },
            detected_by_event=detected_by_event,
            occurred_at=occurred_at,
        )

    def evaluate_topic_risks(
        self,
        topics: List[Topic],
        occurred_at: Optional[int] = None,
        detected_by_event: str = "topic.rank_updated",
    ) -> List[TopicRiskWarning]:
        occur_ts = int(occurred_at or now_timestamp())
        warnings: List[TopicRiskWarning] = []

        for topic in topics:
            if not isinstance(topic, Topic):
                continue
            topic_created_at, topic_id = self._safe_topic_keys(topic)
            if topic_created_at <= 0 or topic_id <= 0:
                continue

            candidates = [
                self._check_negative_cluster(topic, occur_ts, detected_by_event),
                self._check_burst_event(topic, occur_ts, detected_by_event),
                self._check_cross_platform_gap(topic, occur_ts, detected_by_event),
                self._check_sensitive_keywords(topic, occur_ts, detected_by_event),
            ]
            for item in candidates:
                if item is not None:
                    warnings.append(item)

        return warnings

    def evaluate_and_record_topic_risks(
        self,
        topics: List[Topic],
        occurred_at: Optional[int] = None,
        detected_by_event: str = "topic.rank_updated",
    ) -> int:
        warnings = self.evaluate_topic_risks(
            topics=topics,
            occurred_at=occurred_at,
            detected_by_event=detected_by_event,
        )
        if not warnings:
            return 0
        return self.risk_warning_repository.add_topic_risk_warnings(warnings)

    def record_sensitive_title_block(self, payload: Dict[str, Any]) -> bool:
        topic_created_at = self._to_int(payload.get("topic_created_at"), 0)
        topic_id = self._to_int(payload.get("topic_id"), -1)
        if topic_created_at <= 0 or topic_id <= 0:
            return False

        candidate_titles_raw = payload.get("candidate_titles", [])
        candidate_titles: List[str] = []
        if isinstance(candidate_titles_raw, list):
            candidate_titles = [str(item).strip() for item in candidate_titles_raw if str(item).strip()]

        reason_raw = str(payload.get("reason", "data_inspection_failed") or "data_inspection_failed")
        reason_map = {
            "data_inspection_failed": "由于合规性检查（敏感词或道德策略）未通过，LLM 输出内容被拦截",
            "content_filter_blocked": "LLM 触发内容安全过滤机制",
            "audit_failed": "人工/自动审计未通过"
        }
        reason = reason_map.get(reason_raw, f"安全策略拦截: {reason_raw}")

        occurred_at = self._to_int(payload.get("occurred_at"), now_timestamp())
        record = SensitiveTitleRecord(
            topic_created_at=topic_created_at,
            topic_id=topic_id,
            topic_name=str(payload.get("topic_name", "") or ""),
            old_topic=str(payload.get("old_topic", "") or ""),
            candidate_titles=candidate_titles,
            reason=reason,
            risk_level=str(payload.get("risk_level", RISK_LEVEL_HIGH) or RISK_LEVEL_HIGH),
            occurred_at=occurred_at,
            context={
                "error_code": str(payload.get("error_code", "") or ""),
                "blocked": bool(payload.get("blocked", True)),
                "detected_by_event": str(payload.get("detected_by_event", "") or ""),
            },
        )

        inserted = self.risk_warning_repository.add_sensitive_title_records([record])
        return inserted > 0

    def get_topic_risk_warnings(
        self,
        topic_created_at: Optional[int] = None,
        topic_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        risk_level: Optional[str] = None,
        limit: int = 100,
    ) -> List[TopicRiskWarning]:
        return self.risk_warning_repository.get_topic_risk_warnings(
            topic_created_at=topic_created_at,
            topic_id=topic_id,
            start_time=start_time,
            end_time=end_time,
            risk_level=risk_level,
            limit=limit,
        )

    def get_sensitive_title_records(
        self,
        topic_created_at: Optional[int] = None,
        topic_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[SensitiveTitleRecord]:
        return self.risk_warning_repository.get_sensitive_title_records(
            topic_created_at=topic_created_at,
            topic_id=topic_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
