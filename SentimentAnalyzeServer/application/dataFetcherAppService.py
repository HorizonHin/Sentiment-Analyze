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
    def __init__(self, config_path: str | Path, storage: object) -> None:
        self.config_path = Path(config_path)
        
        # 从配置中读取最大评论数
        with self.config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        
        sentiment_config = config.get("sentiment") or {}
        max_comments = int(sentiment_config.get("weibo", {}).get("max_comments", 30))
        
        self.fetcher = DataFetcher(max_comments=max_comments)
        self.news_domain_service = NewsDomainService(storage)
        self.event_manager = EventManager()
        self.redis = MyRedis()
        self.common_thread_pool = CommonThreadPool()

    def _serialize_news_items(self, items: List[NewsItem]) -> List[Dict[str, Any]]:
        return [item.to_dict() for item in items]

    
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

        # 在保存前抓取评论 (异步集成)
        async def fetch_comments_task(item: NewsItem):
            try:
                # 获取 item 的 url，如果为空则尝试使用 mobile_url
                fetch_url = item.url or item.mobile_url
                comments = await self.fetcher.crawl_comments_dispatch(item.source_id, item.title, fetch_url)
                if comments:
                    item.comments = comments
            except Exception as e:
                logger.warning(f"抓取评论失败 {item.source_id} - {item.title}: {e}")

        # 使用 asyncio 运行单次任务 (目前代码中是调试用的单次任务)
        try:
            # 尝试获取当前线程的事件循环
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            if loop.is_running():
                # 如果当前有运行中的事件循环，使用 run_coroutine_threadsafe
                asyncio.run_coroutine_threadsafe(fetch_comments_task(incoming_items[0]), loop).result()
                asyncio.run_coroutine_threadsafe(self.fetcher.close(), loop).result()
            else:
                loop.run_until_complete(fetch_comments_task(incoming_items[0]))
                loop.run_until_complete(self.fetcher.close())
        except Exception as e:
            logger.error(f"异步评论抓取执行出错: {e}")
        key_list = list({(item.source_id, item.title) for item in incoming_items if item.source_id and item.title})
        existing_items = self.news_domain_service.get_news_list_by_source_title_list(
            key_list,
            0,
        )
        existing_item_map = {(item.source_id, item.title): item for item in existing_items}

        new_items_by_source: Dict[str, List[NewsItem]] = {}
        merged_items: List[NewsItem] = []
        for source_id, news_list in current_data.items.items():
            for item in news_list:
                key = (item.source_id, item.title)
                if key in existing_item_map:
                    # 用新数据更新既存项
                    existing_item = existing_item_map[key]
                    self.news_domain_service.applyNewsField(item, existing_item)
                    merged_items.append(existing_item)
                else:
                    new_items_by_source.setdefault(source_id, []).append(item)

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

            # 缓存操作现已全部交给 sentimentAnalyzeAppsService 处理，以避免被覆盖丢失数据
            self.fetcher.close()  # 关闭浏览器，释放资源
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

