import time
import yaml
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
from SentimentAnalyzeServer.domain.news.news import NewsItem

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

def is_item_analysis_pending(item: NewsItem) -> bool:
    """
    判断新闻项是否需要进行情感分析。
    过滤规则：comments 不为空且（未分析过 或 上次分析时间距今已过 2.2 个系统周期）
    """
    if not hasattr(item, 'comments') or not item.comments:
        return False
    
    # 2.2 个系统周期
    interval_seconds = get_interval_seconds()
    lookback_threshold = int(time.time()) - int(2.2 * interval_seconds)
    
    analyzed_time: Optional[datetime] = getattr(item, 'analyzed_time', None)
    if not analyzed_time:
        return True
    
    return int(analyzed_time.timestamp()) < lookback_threshold


