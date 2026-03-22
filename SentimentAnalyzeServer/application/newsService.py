
from datetime import datetime
from typing import Dict, List, Optional

from SentimentAnalyzeServer.domain.llmAnalyzer.llmAnalyzer import LLMTitleAnalyzer
from SentimentAnalyzeServer.domain.news.news import Entity, Keyword, NewsData, NewsItem, NewsDomainService



def convert_crawl_results_to_news_data(
    results: Dict[str, Dict],
    id_to_name: Dict[str, str],
    failed_ids: List[str],
    crawl_time: str,
    crawl_date: str,
) -> NewsData:
    """
    将爬虫结果转换为 NewsData 格式

    Args:
        results: 爬虫返回的结果 {source_id: {title: {ranks: [], url: "", mobileUrl: ""}}}
        id_to_name: 来源ID到名称的映射
        failed_ids: 失败的来源ID
        crawl_time: 抓取时间（HH:MM）
        crawl_date: 抓取日期（YYYY-MM-DD）

    Returns:
        NewsData 对象
    """
    items = {}

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
                crawl_time=crawl_time,
                first_time=crawl_time,
                last_time=crawl_time,
                count=1,
                rank_timeline=[{"time": crawl_time, "rank": latest_rank}],
            )
            news_list.append(news_item)

        items[source_id] = news_list

    return NewsData(
        date=crawl_date,
        crawl_time=crawl_time,
        items=items,
        id_to_name=id_to_name,
        failed_ids=failed_ids,
    )


def _apply_llm_result(item: NewsItem, result: Dict) -> None:
    item.event_type = str(result.get("event_type", ""))
    item.summary = str(result.get("summary", ""))

    entities = result.get("entities", [])
    if isinstance(entities, list):
        item.entities = [
            Entity(name=str(entity.get("name", "")), type=str(entity.get("type", "")))
            for entity in entities
            if isinstance(entity, dict)
        ]

    keywords = result.get("keywords", [])
    if isinstance(keywords, list):
        item.keywords = [
            Keyword(term=str(keyword.get("term", "")), importance=float(keyword.get("importance", 0.0)))
            for keyword in keywords
            if isinstance(keyword, dict)
        ]

    sentiment = result.get("sentiment_analysis", {})
    if isinstance(sentiment, dict):
        item.sentiment_polarity = str(sentiment.get("polarity", ""))
        item.positive_ratio = float(sentiment.get("positive_ratio", 0.0))
        item.negative_ratio = float(sentiment.get("negative_ratio", 0.0))
        item.neutral_ratio = float(sentiment.get("neutral_ratio", 0.0))

        dimensions = sentiment.get("dimensions", {})
        if isinstance(dimensions, dict):
            item.optimism_score = float(dimensions.get("optimism", 0.0))
            item.trust_score = float(dimensions.get("trust", 0.0))
            item.attention_score = float(dimensions.get("attention", 0.0))
            item.controversy_score = float(dimensions.get("controversy", 0.0))

    item.analyzed_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def analyze_news_items_if_needed(
    items: List[NewsItem],
    analyzer: Optional[LLMTitleAnalyzer] = None,
) -> List[NewsItem]:
    analyzer = analyzer or LLMTitleAnalyzer()

    for item in items:
        if item.analyzed_time or item.sentiment_polarity or item.entities or item.keywords:
            continue
        result = analyzer.analyze_title(item.title)
        _apply_llm_result(item, result)

    return items


def _merge_rank_info(existing: NewsData, current: NewsData) -> None:
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
            item.count = old_item.count + item.count

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
    results: Dict[str, Dict],
    id_to_name: Dict[str, str],
    failed_ids: List[str],
    crawl_time: str,
    crawl_date: str,
    storage: object,
    analyzer: Optional[LLMTitleAnalyzer] = None,
) -> tuple[NewsData, bool]:
    domain_service = NewsDomainService(storage)
    current_data = convert_crawl_results_to_news_data(
        results=results,
        id_to_name=id_to_name,
        failed_ids=failed_ids,
        crawl_time=crawl_time,
        crawl_date=crawl_date,
    )

    existing_data = domain_service.get_today_all_data(crawl_date)
    if existing_data:
        _merge_rank_info(existing_data, current_data)

    saved = domain_service.save_news_data(current_data)
    return current_data, saved