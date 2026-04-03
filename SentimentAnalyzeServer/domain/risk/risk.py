from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import time
from typing import Any, Dict, List, Optional

from SentimentAnalyzeServer.domain.topic.topic import Topic, TopicPlatformStats

RISK_TYPE_NEGATIVE_CLUSTER = "negative_cluster"
RISK_TYPE_BURST_EVENT = "burst_event"
RISK_TYPE_CROSS_PLATFORM_GAP = "cross_platform_gap"

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
                f"negative_ratio={negative_ratio:.3f} >= {self.negative_ratio_threshold:.2f} "
                f"and news_count={news_count} >= {self.negative_news_count_threshold}"
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
        return TopicRiskWarning(
            topic_created_at=topic_created_at,
            topic_id=topic_id,
            topic_name=str(getattr(topic, "topic", "") or ""),
            risk_type=RISK_TYPE_BURST_EVENT,
            risk_level=self._score_to_level(score),
            risk_score=score,
            reason=(
                f"heat_change_percent={heat_change_percent:.2f} >= {self.burst_heat_change_threshold:.2f} "
                f"and stage={stage}"
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
        max_score = max(score_values)
        min_score = min(score_values)
        gap = max_score - min_score
        has_positive = any(value > 0.2 for value in score_values)
        has_negative = any(value < -0.2 for value in score_values)
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
                f"platform_sentiment_gap={gap:.3f} >= {self.cross_platform_gap_threshold:.2f} "
                f"and polarity_conflict={polarity_conflict}"
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

        reason = str(payload.get("reason", "data_inspection_failed") or "data_inspection_failed")
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
