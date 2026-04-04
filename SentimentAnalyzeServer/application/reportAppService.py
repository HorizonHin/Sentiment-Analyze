from __future__ import annotations

import os
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from SentimentAnalyzeServer.domain.topic.topic import Topic, TopicDomainService
from SentimentAnalyzeServer.domain.risk.risk import TopicRiskWarning, RiskWarningDomainService, RISK_LEVEL_CRITICAL, RISK_LEVEL_HIGH
from SentimentAnalyzeServer.domain.news.news import NewsDomainService

@dataclass(slots=True)
class DailyReport:
    start_time: int
    end_time: int
    total_active_topics: int
    top_topics: List[Topic] = field(default_factory=list)
    risk_warnings: List[TopicRiskWarning] = field(default_factory=list)
    sentiment_stats: Dict[str, int] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "total_active_topics": self.total_active_topics,
            "top_topics": [t.to_dict() for t in self.top_topics],
            "risk_warnings": [
                {
                    "topic_name": w.topic_name,
                    "risk_type": w.risk_type,
                    "risk_level": w.risk_level,
                    "reason": w.reason,
                    "occurred_at": w.occurred_at
                } for w in self.risk_warnings
            ],
            "sentiment_stats": self.sentiment_stats
        }

class ReportAppService:
    def __init__(
        self,
        topic_domain_service: TopicDomainService,
        risk_warning_domain_service: RiskWarningDomainService,
        news_domain_service: NewsDomainService,
        system_dir: Optional[str] = None,
    ) -> None:
        self.topic_domain_service = topic_domain_service
        self.risk_warning_domain_service = risk_warning_domain_service
        self.news_domain_service = news_domain_service
        
        # 报告存储目录
        if system_dir:
            self.report_dir = Path(system_dir) / "daily_reports"
        else:
            self.report_dir = Path(os.path.dirname(__file__)).parent / "system" / "daily_reports"
        
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def generate_and_save_daily_report(self, end_time: Optional[int] = None) -> Tuple[bool, str]:
        """
        生成日报并保存到文件系统
        """
        report = self.generate_daily_report_obj(end_time)
        date_str = time.strftime("%Y%m%d", time.localtime(report.end_time))
        file_path = self.report_dir / f"report_{date_str}.json"
        
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
            return True, str(file_path)
        except Exception as e:
            return False, f"Save failed: {str(e)}"

    def get_report_by_date(self, date_str: str) -> Optional[Dict[str, Any]]:
        """
        根据日期获取日报 (格式: YYYYMMDD)
        """
        file_path = self.report_dir / f"report_{date_str}.json"
        if not file_path.exists():
            return None
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def generate_daily_report_obj(self, end_time: Optional[int] = None) -> DailyReport:
        """
        生成过去24小时的日报
        """
        now = int(end_time or time.time())
        day_seconds = 24 * 3600
        start_time = now - day_seconds

        # 1. 获取过去24小时活跃（更新过）的话题
        # 注意：list_topics_by_time_range 支持 updated_at_start
        active_topics = self.topic_domain_service.list_topics_by_time_range(
            updated_at_start=start_time,
            updated_at_end=now,
            limit=500
        )

        # 2. 统计情感分布和排序
        sentiment_dist: Dict[str, int] = {}
        for t in active_topics:
            s = t.sentiment or "unknown"
            sentiment_dist[s] = sentiment_dist.get(s, 0) + 1
        
        # 按热度排序获取 Top 10
        sorted_topics = sorted(active_topics, key=lambda x: x.total_weight, reverse=True)
        top_10 = sorted_topics[:10]

        # 3. 获取风险预警
        # get_topic_risk_warnings 支持 start_time 和 end_time
        critical_risks = self.risk_warning_domain_service.get_topic_risk_warnings(
            start_time=start_time,
            end_time=now,
            limit=100
        )
        # 过滤出高及以上程度的风险
        important_risks = [r for r in critical_risks if r.risk_level in {RISK_LEVEL_CRITICAL, RISK_LEVEL_HIGH}]

        return DailyReport(
            start_time=start_time,
            end_time=now,
            total_active_topics=len(active_topics),
            top_topics=top_10,
            risk_warnings=important_risks,
            sentiment_stats=sentiment_dist
        )

    def format_to_markdown(self, report: DailyReport) -> str:
        """
        将报告格式化为 Markdown
        """
        start_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(report.start_time))
        end_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(report.end_time))
        
        lines = []
        lines.append(f"# 舆情日报 ({end_str.split(' ')[0]})")
        lines.append(f"\n**时间范围**: `{start_str}` — `{end_str}`")
        lines.append(f"\n## 1. 总体概况")
        lines.append(f"- **活跃话题总数**: {report.total_active_topics}")
        
        sent_parts = []
        for s, count in report.sentiment_stats.items():
            sent_parts.append(f"{s}: {count}")
        lines.append(f"- **情感分布**: {', '.join(sent_parts)}")

        lines.append(f"\n## 2. 热度 Top 10 话题")
        if not report.top_topics:
            lines.append("*暂无活跃话题*")
        else:
            lines.append("| 排名 | 话题名称 | 热度值 | 阶段 | 情感 |")
            lines.append("| :--- | :--- | :--- | :--- | :--- |")
            for i, t in enumerate(report.top_topics, 1):
                name = t.llm_title or t.topic
                lines.append(f"| {i} | {name} | {t.total_weight:.1f} | {t.stage} | {t.sentiment} |")

        lines.append(f"\n## 3. 风险预警 (高/危)")
        if not report.risk_warnings:
            lines.append("*过去24小时内未发现高风险项*")
        else:
            for r in report.risk_warnings:
                level_mark = "🔴" if r.risk_level == RISK_LEVEL_CRITICAL else "🟠"
                lines.append(f"- {level_mark} **[{r.risk_level.upper()}]** {r.topic_name}")
                lines.append(f"  - 原因: {r.reason}")
                occ_str = time.strftime("%H:%M", time.localtime(r.occurred_at))
                lines.append(f"  - 触发时间: {occ_str}")

        lines.append(f"\n---\n*Report generated at {time.strftime('%Y-%m-%d %H:%M:%S')}*")
        return "\n".join(lines)
