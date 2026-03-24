
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

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
        self.news_domain_service = NewsDomainService(storage)

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
        try:
            saved_items = self.convert_crawl_results_and_save(
                results=results,
                id_to_name=id_to_name,
                failed_ids=failed_ids,
                last_time=last_time,
                crawl_date=crawl_date,
            )
        except Exception:
            return {
                "success": False,
                "reason": "save_failed",
                "platform_count": len(ids),
                "success_count": len(results),
                "failed_count": len(failed_ids),
                "failed_ids": failed_ids,
                "id_to_name": id_to_name,
            }, []

        return {
            "success": True,
            "platform_count": len(ids),
            "success_count": len(results),
            "failed_count": len(failed_ids),
            "failed_ids": failed_ids,
            "id_to_name": id_to_name,
        }, saved_items

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
    
    def _group_items_by_source(self, items: List[NewsItem]) -> Dict[str, List[NewsItem]]:
        grouped: Dict[str, List[NewsItem]] = {}
        for item in items:
            grouped.setdefault(item.source_id, []).append(item)
        return grouped

    def convert_crawl_results_and_save(
        self,
        results: Dict[str, Dict],
        id_to_name: Dict[str, str],
        failed_ids: List[str],
        last_time: str,
        crawl_date: str,
    ) -> List[NewsItem]:
        current_data = self.convert_crawl_results_to_news_data(
            results=results,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
            last_time=last_time,
            crawl_date=crawl_date,
        )

        incoming_items: List[NewsItem] = []
        for news_list in current_data.items.values():
            incoming_items.extend(news_list)

        if not incoming_items:
            return self.news_domain_service.add_news_items(incoming_items)

        key_list = list({(item.source_id, item.title) for item in incoming_items if item.source_id and item.title})
        existing_items = self.news_domain_service.get_news_list_by_source_title_list(key_list)
        existing_keys = {(item.source_id, item.title) for item in existing_items}

        new_items_by_source: Dict[str, List[NewsItem]] = {}
        existing_news_items: List[NewsItem] = []
        for source_id, news_list in current_data.items.items():
            for item in news_list:
                if (item.source_id, item.title) in existing_keys:
                    existing_news_items.append(item)
                else:
                    new_items_by_source.setdefault(source_id, []).append(item)

        saved_items: List[NewsItem] = []

        if new_items_by_source:
            new_items: List[NewsItem] = []
            for grouped_items in new_items_by_source.values():
                new_items.extend(grouped_items)
            added_items = self.news_domain_service.add_news_items(new_items)
            if not added_items:
                raise RuntimeError("保存新增新闻数据失败")
            saved_items.extend(added_items)

        if existing_news_items:
            updated_items = self.news_domain_service.update_existing_crawled_titles(existing_news_items)
            if not updated_items:
                raise RuntimeError("更新已存在新闻数据失败")
            saved_items.extend(updated_items)

        return saved_items