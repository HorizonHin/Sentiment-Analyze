# coding=utf-8
"""Flask 应用入口"""

from __future__ import annotations

import sys
import atexit
from pathlib import Path
from typing import Any

from flask import Flask, jsonify
import yaml
from SentimentAnalyzeServer.application.scheduled import (
    Scheduled,
    get_interval_seconds_from_config,
)
from SentimentAnalyzeServer.application.common import Result
from SentimentAnalyzeServer.application.dataFetcherAppService import DataFetcherAppService
from SentimentAnalyzeServer.application.sentimentAnalyzeAppsService import SentimentAnalyzeAppService
from SentimentAnalyzeServer.application.topicAppService import TopicAppService
from SentimentAnalyzeServer.domain.llmAnalyzer.llmAnalyzer import LLMTitleAnalyzer
from SentimentAnalyzeServer.domain.news.news import NewsDomainService
from SentimentAnalyzeServer.domain.news.sqlServerNewsItemRepository import SqlServerNewsItemRepository
from SentimentAnalyzeServer.domain.topic.topic import TopicDomainService
from SentimentAnalyzeServer.domain.topic.sqlServerTopicRepository import SqlServerTopicRepository
from SentimentAnalyzeServer.inbound.controller import create_external_controller
from SentimentAnalyzeServer.inbound.workflow_event_subscribers import WorkflowEventSubscribers
from SentimentAnalyzeServer.system.infra import CommonThreadPool


def create_app() -> Flask:
    app = Flask(__name__)

    root_dir = Path(__file__).resolve().parent.parent
    config_path = root_dir / "config.yaml"
    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    llm_executor_config = config.get("llm_executor") or {}
    try:
        llm_max_workers = max(1, int(llm_executor_config.get("max_workers", 32)))
    except (TypeError, ValueError):
        llm_max_workers = 32

    interval_seconds = get_interval_seconds_from_config(config)

    common_thread_pool_config = config.get("common_thread_pool") or {}
    try:
        common_thread_pool_max_workers = max(1, int(common_thread_pool_config.get("max_workers", 8)))
    except (TypeError, ValueError):
        common_thread_pool_max_workers = 8
    CommonThreadPool.configure(common_thread_pool_max_workers)

    llm_config = config.get("llm") or {}
    llm_api_key = str(llm_config.get("api_key", "")).strip()
    topic_heat_stage_config = (config.get("topic") or {}).get("heat_stage") or {}
    
    # MSSQL 配置优先级: 配置文件 > 默认值
    mssql_config = config.get("mssql") or {}
    mssql_server = mssql_config.get("server") or "localhost"
    mssql_database = mssql_config.get("database") or "sentiment_analyze"
    mssql_username = mssql_config.get("username") or "18020"
    mssql_password = mssql_config.get("password") or ""
    mssql_driver = mssql_config.get("driver") or "ODBC Driver 17 for SQL Server"
    
    # 初始化数据库存储后端，失败则停止应用
    try:
        storage = SqlServerNewsItemRepository(
            server=mssql_server,
            database=mssql_database,
            username=mssql_username,
            password=mssql_password,
            driver=mssql_driver,
        )
    except RuntimeError as e:
        print(f"[错误] {e}", file=sys.stderr)
        sys.exit(1)
    
    data_fetcher_app_service = DataFetcherAppService(config_path=config_path, storage=storage)
    topic_repository = SqlServerTopicRepository(
        server=mssql_server,
        database=mssql_database,
        username=mssql_username,
        password=mssql_password,
        driver=mssql_driver,
    )
    sentiment_app_service = SentimentAnalyzeAppService(
        storage=storage,
        analyzer=LLMTitleAnalyzer(api_key=llm_api_key),
        max_workers=llm_max_workers,
        recent_window_seconds=interval_seconds,
    )
    topic_app_service = TopicAppService(
        topic_domain_service=TopicDomainService(
            topic_repository=topic_repository,
            heat_stage_config=topic_heat_stage_config,
        ),
        news_domain_service=NewsDomainService(storage),
        crawl_interval_seconds=interval_seconds,
        topic_config=config.get("topic") or {},
    )
    workflow_subscribers = WorkflowEventSubscribers(
        sentiment_app_service=sentiment_app_service,
        topic_app_service=topic_app_service,
        crawl_interval_seconds=interval_seconds,
    )
    workflow_subscribers.register()

    app.register_blueprint(
        create_external_controller(
            sentiment_app_service=sentiment_app_service,
            topic_app_service=topic_app_service,
            crawl_interval_seconds=interval_seconds,
        )
    )

    scheduler = Scheduled(
        config_path=config_path,
        storage=storage,
        interval_seconds=interval_seconds,
        llm_max_workers=llm_max_workers,
        data_fetcher_app_service=data_fetcher_app_service,
        sentiment_app_service=sentiment_app_service,
        topic_app_service=topic_app_service,
    )

    app.config["scheduler"] = scheduler
    scheduler.start()

    def _shutdown_scheduler() -> None:
        scheduler.stop()

    atexit.register(_shutdown_scheduler)

    @app.get("/health")
    def health() -> Any:
        return jsonify(Result.success_result({"status": "ok"}).to_dict())
    return app

app = create_app()


# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000, debug=False)
