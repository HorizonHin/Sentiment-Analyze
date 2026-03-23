
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from SentimentAnalyzeServer.domain.crawler import DataFetcher
from SentimentAnalyzeServer.domain.news.news import (
    NewsData,
    NewsItem,
    NewsDomainService,
)


class DataFetcherAppService:
    def __init__(self, config_path: str | Path, storage: object) -> None:
        self.config_path = Path(config_path)
        self.fetcher = DataFetcher()
        self.domain_service = NewsDomainService(storage)

    def _load_platforms(self) -> list[tuple[str, str]]:
        with self.config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        sources = (config.get("platforms") or {}).get("sources") or []
        ids: list[tuple[str, str]] = []
        for item in sources:
            if not isinstance(item, dict):
                continue
            platform_id = str(item.get("id", "")).strip()
            if not platform_id:
                continue
            name = str(item.get("name", platform_id)).strip() or platform_id
            ids.append((platform_id, name))

        return ids

    def crawl_and_save_news_data(self) -> tuple[dict[str, Any], List[NewsItem]]:
        ids = self._load_platforms()
        if not ids:
            print("[ScheduledCrawler] 未在配置中找到可抓取平台")
            return {"success": False, "reason": "no_platforms"}, []

        print(f"[ScheduledCrawler] 开始抓取，平台数: {len(ids)}")
        results, id_to_name, failed_ids = self.fetcher.crawl_websites(ids)
        print(
            f"[ScheduledCrawler] 抓取完成，成功: {len(results)}，失败: {len(failed_ids)}"
        )

        now = datetime.now()
        crawl_date = now.strftime("%Y-%m-%d")
        last_time = now.strftime("%Y-%m-%d %H:%M")
        current_data, saved_data = self.convert_crawl_results_and_save(
            results=results,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
            last_time=last_time,
            crawl_date=crawl_date,
        )

        if saved_data is None:
            return {
                "success": False,
                "reason": "save_failed",
                "platform_count": len(ids),
                "success_count": len(results),
                "failed_count": len(failed_ids),
                "failed_ids": failed_ids,
                "id_to_name": id_to_name,
            }, []

        effective_data = saved_data or current_data
        all_items: List[NewsItem] = []
        for news_list in effective_data.items.values():
            all_items.extend(news_list)

        return {
            "success": True,
            "platform_count": len(ids),
            "success_count": len(results),
            "failed_count": len(failed_ids),
            "failed_ids": failed_ids,
            "id_to_name": id_to_name,
        }, all_items

    def convert_crawl_results_to_news_data(
        self,
        results: Dict[str, Dict],
        id_to_name: Dict[str, str],
        failed_ids: List[str],
        last_time: str,
        crawl_date: str,
    ) -> NewsData:
        items: Dict[str, List[NewsItem]] = {}

        for source_id, titles_data in results.items():
            source_name = id_to_name.get(source_id, source_id)
            news_list = []

            for title, data in titles_data.items():
                ranks = data.get("ranks", [])
                url = data.get("url", "")
                mobile_url = data.get("mobileUrl", "")

                latest_rank = ranks[0] if ranks else 99

                news_item = NewsItem(
                    title=title,
                    source_id=source_id,
                    source_name=source_name,
                    latest_rank=latest_rank,
                    url=url,
                    mobile_url=mobile_url,
                    first_time=last_time,
                    last_time=last_time,
                    rank_timeline=[{"time": last_time, "rank": latest_rank}],
                )
                news_list.append(news_item)

            items[source_id] = news_list

        return NewsData(
            date=crawl_date,
            last_time=last_time,
            items=items,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
        )

    def _merge_rank_info(self, existing: NewsData, current: NewsData) -> None:
        existing_map: Dict[tuple[str, str], NewsItem] = {}
        for source_id, news_list in existing.items.items():
            for item in news_list:
                existing_map[(source_id, item.title)] = item

        for source_id, news_list in current.items.items():
            for item in news_list:
                key = (source_id, item.title)
                old_item = existing_map.get(key)
                if not old_item:
                    continue

                combined_timeline = old_item.rank_timeline + item.rank_timeline
                item.rank_timeline = sorted(
                    combined_timeline,
                    key=lambda x: (x.get("time", ""), x.get("rank") is None, x.get("rank", 0)),
                )

                if old_item.first_time and (not item.first_time or old_item.first_time < item.first_time):
                    item.first_time = old_item.first_time
                if old_item.last_time and (not item.last_time or old_item.last_time > item.last_time):
                    item.last_time = old_item.last_time
                if not item.summary and old_item.summary:
                    item.summary = old_item.summary
                if not item.event_type and old_item.event_type:
                    item.event_type = old_item.event_type
                if not item.entities and old_item.entities:
                    item.entities = old_item.entities
                if not item.keywords and old_item.keywords:
                    item.keywords = old_item.keywords
                if not item.sentiment_polarity and old_item.sentiment_polarity:
                    item.sentiment_polarity = old_item.sentiment_polarity
                    item.positive_ratio = old_item.positive_ratio
                    item.negative_ratio = old_item.negative_ratio
                    item.neutral_ratio = old_item.neutral_ratio
                    item.optimism_score = old_item.optimism_score
                    item.trust_score = old_item.trust_score
                    item.attention_score = old_item.attention_score
                    item.controversy_score = old_item.controversy_score
                    item.analyzed_time = old_item.analyzed_time

    def convert_crawl_results_and_save(
        self,
        results: Dict[str, Dict],
        id_to_name: Dict[str, str],
        failed_ids: List[str],
        last_time: str,
        crawl_date: str,
    ) -> tuple[NewsData, Optional[NewsData]]:
        current_data = self.convert_crawl_results_to_news_data(
            results=results,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
            last_time=last_time,
            crawl_date=crawl_date,
        )
        saved_data = self.domain_service.add_news_data(current_data)
        return current_data, saved_data