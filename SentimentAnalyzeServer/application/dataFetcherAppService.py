import logging
from pathlib import Path
import time
import asyncio
from typing import Any, Dict, List

import yaml

from SentimentAnalyzeServer.system.infra import (
    EVENT_CRAWL_SAVED,
    REDIS_KEY_LATEST_NOT_NEED_ANALYSIS_NEWS,
    CommonThreadPool,
    EventManager,
    MyRedis,
)
from SentimentAnalyzeServer.application.common import is_item_analysis_pending
from SentimentAnalyzeServer.domain.crawler import DataFetcher
from SentimentAnalyzeServer.domain.news.news import (
    NewsData,
    NewsItem,
    NewsDomainService,
    RankTimelineEntry,
)


logger = logging.getLogger(__name__)


class DataFetcherAppService:
    _CRAWL_RUN_LOCK_KEY = "lock:data_fetcher:crawl_and_save_news_data"
    _CRAWL_RUN_LOCK_TTL_SECONDS = 30 * 60

    def __init__(self, config_path: str | Path, storage: object) -> None:
        self.config_path = Path(config_path)
        
        # 从配置中读取最大评论数和回溯天数
        with self.config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        
        sentiment_config = config.get("sentiment") or {}
        max_comments = int(sentiment_config.get("weibo", {}).get("max_comments", 30))
        self._first_time_lookback_days = int(sentiment_config.get("first_time_lookback_days", 3))
        
        # 线程池配置：默认4个worker，每个worker处理3个平台
        self._comment_worker_count = 4
        self._platforms_per_worker = 3

        self.fetcher = DataFetcher(max_comments=max_comments)
        self.news_domain_service = NewsDomainService(storage)
        self.event_manager = EventManager()
        self.redis = MyRedis()
        self.common_thread_pool = CommonThreadPool()

    async def _run_comment_fetch_task(self, incoming_items: List[NewsItem]) -> None:
        """执行评论抓取主任务（纯协程实现，平台间并发，平台内串行）。"""
        if not incoming_items:
            return

        # 1. 按平台分组
        platform_groups: Dict[str, List[NewsItem]] = {}
        for item in incoming_items:
            platform_groups.setdefault(item.source_id, []).append(item)

        async def fetch_item_comments(item: NewsItem):
            try:
                fetch_url = item.url or item.mobile_url
                if not fetch_url:
                    return
                comments = await self.fetcher.crawl_comments_dispatch(item.source_id, item.title, fetch_url)
                if comments:
                    item.comments = comments
            except Exception as e:
                logger.warning(f"抓取评论失败 {item.source_id} - {item.title}: {e}")

        async def fetch_platform_group(items: List[NewsItem]):
            # 平台内串行：同一时刻一个平台只抓取一个
            for item in items:
                await fetch_item_comments(item)

        try:
            # 2. 平台之间全并发
            tasks = [fetch_platform_group(items) for items in platform_groups.values()]
            await asyncio.gather(*tasks)
        finally:
            # 任务结束后关闭渲染客户端
            await self.fetcher.close()

    async def _fetch_comments_by_platform(self, incoming_items: List[NewsItem]) -> None:
        """
        按照平台(source_id)分配并发任务，每个平台内的抓取任务在各自的协程中运行。
        平台之间全并发；同一平台同一时间只执行一个抓取任务。
        """
        if not incoming_items:
            return

        await self._run_comment_fetch_task(incoming_items)

    def _convert_crawl_results_to_news_data(
        self,
        results: Dict[str, Dict],
        id_to_name: Dict[str, str],
        failed_ids: List[str],
        last_time: int,
        crawl_date: int,
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
                try:
                    latest_rank = int(latest_rank)
                except (TypeError, ValueError):
                    latest_rank = 99

                news_item = NewsItem(
                    title=title,
                    source_id=source_id,
                    source_name=source_name,
                    latest_rank=latest_rank,
                    url=url,
                    mobile_url=mobile_url,
                    first_time=last_time,
                    last_time=last_time,
                    rank_timeline_obj=[RankTimelineEntry(time=last_time, rank=latest_rank)],
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

    def convert_crawl_results_and_save(
        self,
        results: Dict[str, Dict],
        id_to_name: Dict[str, str],
        failed_ids: List[str],
        last_time: int,
        crawl_date: int,
    ) -> List[NewsItem]:
        current_data = self._convert_crawl_results_to_news_data(
            results=results,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
            last_time=last_time,
            crawl_date=crawl_date,
        )
        current_data.merge_duplicate_titles_by_source()

        incoming_items: List[NewsItem] = []
        for news_list in current_data.items.values():
            incoming_items.extend(news_list)

        if not incoming_items:
            return self.news_domain_service.add_news_items(incoming_items)

        # 1. 调整顺序：先查数据库，判断哪些新闻是已存在的
        source_title_list = list({(item.source_id, item.title) for item in incoming_items if item.source_id and item.title})
        url_list = list({item.url for item in incoming_items if item.url})
        mobile_url_list = list({item.mobile_url for item in incoming_items if item.mobile_url})
        all_urls = list(set(url_list + mobile_url_list))

        existing_items_by_st = self.news_domain_service.get_news_list_by_source_title_list(source_title_list)
        existing_items_by_url = self.news_domain_service.get_news_list_by_url(all_urls)

        existing_item_map: Dict[tuple[str, str], NewsItem] = {}
        existing_url_map: Dict[str, NewsItem] = {}

        for item in existing_items_by_st:
            existing_item_map[(item.source_id, item.title)] = item
            if item.url:
                existing_url_map[item.url] = item
            if item.mobile_url:
                existing_url_map[item.mobile_url] = item

        for item in existing_items_by_url:
            existing_item_map[(item.source_id, item.title)] = item
            if item.url:
                existing_url_map[item.url] = item
            if item.mobile_url:
                existing_url_map[item.mobile_url] = item

        # 2. 所有新闻都抓评论；仅调整顺序：优先处理尚未具备完整分析结果的新闻。
        def _comment_priority(item: NewsItem) -> int:
            existing = existing_item_map.get((item.source_id, item.title))
            if not existing and item.url:
                existing = existing_url_map.get(item.url)
            if not existing and item.mobile_url:
                existing = existing_url_map.get(item.mobile_url)

            if existing and existing.sentiment_polarity and existing.positive_ratio > 0:
                return 1
            return 0

        prioritized_items = sorted(incoming_items, key=_comment_priority)

        # 3. 执行评论抓取（不修改 NewsItem 结构，只按顺序调度）
        async def run_fetch():
            await self._fetch_comments_by_platform(prioritized_items)
        asyncio.run(run_fetch())
            
        new_items_by_source: Dict[str, List[NewsItem]] = {}
        merged_items: List[NewsItem] = []
        for source_id_from_dict, news_list in current_data.items.items():
            for item in news_list:
                existing_item = existing_item_map.get((item.source_id, item.title))
                if not existing_item and item.url:
                    existing_item = existing_url_map.get(item.url)
                if not existing_item and item.mobile_url:
                    existing_item = existing_url_map.get(item.mobile_url)

                if existing_item:
                    # 用新数据更新既存项
                    self.news_domain_service.applyNewsField(item, existing_item)
                    merged_items.append(existing_item)
                else:
                    new_items_by_source.setdefault(source_id_from_dict, []).append(item)

        saved_items: List[NewsItem] = []

        if new_items_by_source:
            new_items: List[NewsItem] = []
            for grouped_items in new_items_by_source.values():
                new_items.extend(grouped_items)
            # 去重 entities 和 keywords
            for item in new_items:
                item.deduplicate_entities_and_keywords()
            added_items = self.news_domain_service.add_news_items(new_items)
            if not added_items:
                raise RuntimeError("保存新增新闻数据失败")
            saved_items.extend(added_items)

        if merged_items:
            # 去重 entities 和 keywords
            for item in merged_items:
                item.deduplicate_entities_and_keywords()
            updated_items = self.news_domain_service.update_existing_crawled_titles(merged_items)
            if not updated_items:
                raise RuntimeError("更新已存在新闻数据失败")
            saved_items.extend(updated_items)

        if saved_items:
            self.event_manager.publish(
                EVENT_CRAWL_SAVED,
                {
                    "saved_items": saved_items,
                    "last_time": last_time,
                    "crawl_date": crawl_date,
                },
            )

        return saved_items

    def crawl_and_save_news_data(self) -> tuple[dict[str, Any], List[NewsItem]]:
        lock_token = self.redis.acquire_lock(
            self._CRAWL_RUN_LOCK_KEY,
            self._CRAWL_RUN_LOCK_TTL_SECONDS,
        )
        if lock_token is None:
            logger.warning("[dataFetcher] crawl_and_save_news_data 正在运行，本次调用已跳过")
            return {"success": False, "reason": "crawl_running"}, []

        try:
            ids = self._load_platforms()
            if not ids:
                logger.error("[dataFetcher] 未在配置中找到可抓取平台")
                return {"success": False, "reason": "no_platforms"}, []

            results, id_to_name, failed_ids = self.fetcher.crawl_websites(ids)
            last_time = int(time.time())
            crawl_date = last_time - (last_time % 86400)
            try:
                saved_items = self.convert_crawl_results_and_save(
                    results=results,
                    id_to_name=id_to_name,
                    failed_ids=failed_ids,
                    last_time=last_time,
                    crawl_date=crawl_date,
                )
            except Exception as exc:
                logger.exception("抓取数据转换或保存失败: %s", exc.with_traceback(exc.__traceback__))
                return {
                    "success": False,
                    "reason": "save_failed",
                    "platform_count": len(ids),
                    "success_count": len(results),
                    "failed_count": len(failed_ids),
                    "failed_ids": failed_ids,
                    "id_to_name": id_to_name,
                }, []

            logger.info(
                "[dataFetcher] 抓取并入库成功。platform_count=%s, success_count=%s, failed_count=%s, saved_count=%s",
                len(ids),
                len(results),
                len(failed_ids),
                len(saved_items),
            )
            return {
                "success": True,
                "platform_count": len(ids),
                "success_count": len(results),
                "failed_count": len(failed_ids),
                "failed_ids": failed_ids,
                "id_to_name": id_to_name,
            }, saved_items
        finally:
            self.redis.release_lock(self._CRAWL_RUN_LOCK_KEY, lock_token)

