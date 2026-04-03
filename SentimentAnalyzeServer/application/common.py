import time
import yaml
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
from SentimentAnalyzeServer.domain.news.news import NewsItem
from SentimentAnalyzeServer.domain.crawler.fetcher import DataFetcher

@dataclass(slots=True)
class Result:
    # 直接定义属性，dataclass 会自动生成 __init__
    success: bool
    data: Any = None
    error_message: str = ""

    @classmethod
    def success_result(cls, data: Any = None) -> 'Result':
        # 使用 cls(…) 而不是 Result(…) 更加符合面向对象习惯（支持继承）
        return cls(success=True, data=data)

    @classmethod
    def failure_result(cls, error_message: str) -> 'Result':
        return cls(success=False, error_message=error_message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "data": self.data,
            "error_message": self.error_message,
        }

def get_config() -> Dict[str, Any]:
    """获取根目录下的 config.yaml 配置"""
    # 假设 common.py 在 application 目录下，根目录是其父目录
    root_dir = Path(__file__).resolve().parent.parent
    config_path = root_dir / "config.yaml"
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def get_interval_seconds() -> int:
    """从配置中获取系统周期（秒）"""
    config = get_config()
    scheduler_config = config.get("scheduler") or {}
    raw = str(scheduler_config.get("crawl_interval_minutes", "30")).strip()
    try:
        minutes = max(1, int(raw))
    except (ValueError, TypeError):
        minutes = 30
    return minutes * 60

def is_source_support_comments(source_id: str) -> bool:
    """判断该数据源是否属于能抓取评论的源"""
    sid = str(source_id).lower()
    return any(platform in sid for platform in DataFetcher.SUPPORTED_COMMENT_PLATFORMS)

def is_item_analysis_pending(item: NewsItem) -> bool:
    """
    判断新闻项是否需要进行情感/实体分析。
    过滤规则：
    1. 如果支持抓取评论的源：
       - 若尚未分析过 (analyzed_time 为空)，则需要分析。
       - 若已分析过，但上次只有标题分析 (无 summary 或特定标记)，且本次抓取到了评论，则需要立即重新分析以提升质量。
       - 若已分析过且已有 summary，但距今已过 3.2 个系统周期，则需要定期更新。
       - 特殊风险：如果本次没抓到评论，且上次已经有 summary 了，则不应分析（避免用仅标题的结果覆盖高质量的结果）。
    2. 如果不支持评论的源：
       - 只分析一次标题即可。
    """
    source_id = item.source_id
    analyzed_time: Optional[datetime] = item.analyzed_time
    has_comments = bool(getattr(item, 'comments', None))
    has_summary = bool(getattr(item, 'summary', None))

    if is_source_support_comments(source_id):
        # 如果已经有分析结果了 (summary)，但这次却没有评论，绝对不要再次分析（防止降级覆盖）
        if has_summary and not has_comments:
            return False
            
        # 如果没分析过，当然要分析
        if not analyzed_time:
            return True
            
        # 如果上次没出结果 (没有 summary) 且这次有评论，立即分析
        if not has_summary and has_comments:
            return True
            
        # 即使有结果，如果过了很久，也需要更新
        interval_seconds = get_interval_seconds()
        lookback_threshold = int(time.time()) - int(3.2 * interval_seconds)
        if int(analyzed_time.timestamp()) < lookback_threshold:
            return True
            
        return False
    else:
        # 不支持评论的来源，只分析标题，确保分析一次且有了结果就不再动
        if analyzed_time is not None and has_summary:
            return False
        return not analyzed_time


