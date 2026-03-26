# coding=utf-8
"""Flask 应用入口"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from flask import Flask, jsonify
import yaml
from SentimentAnalyzeServer.application.scheduled import (
    Scheduled,
    get_interval_seconds_from_env,
)
from SentimentAnalyzeServer.application.common import Result
from SentimentAnalyzeServer.application.dataFetcherAppService import DataFetcherAppService
from SentimentAnalyzeServer.application.sentimentAnalyzeAppsService import SentimentAnalyzeAppService
from SentimentAnalyzeServer.application.topicAppService import TopicAppService
from SentimentAnalyzeServer.domain.llmAnalyzer.llmAnalyzer import LLMTitleAnalyzer
from SentimentAnalyzeServer.domain.news.news import NewsDomainService
from SentimentAnalyzeServer.domain.news.sqlServerNewsItemRepository import SqlServerNewsItemRepository
from SentimentAnalyzeServer.domain.topic.topic import TopicDomainService
from SentimentAnalyzeServer.inbound.controller import create_external_controller
from SentimentAnalyzeServer.inbound.workflow_event_subscribers import WorkflowEventSubscribers


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
    
    # MSSQL 配置优先级: 配置文件 > 环境变量 > 默认值
    mssql_config = config.get("mssql") or {}
    mssql_server = mssql_config.get("server") or os.getenv("MSSQL_SERVER") or "localhost"
    mssql_database = mssql_config.get("database") or os.getenv("MSSQL_DATABASE") or "sentiment_analyze"
    mssql_username = mssql_config.get("username") or os.getenv("MSSQL_USERNAME") or "18020"
    mssql_password = mssql_config.get("password") or os.getenv("MSSQL_PASSWORD") or ""
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
    sentiment_app_service = SentimentAnalyzeAppService(
        storage=storage,
        analyzer=LLMTitleAnalyzer(),
        max_workers=llm_max_workers,
    )
    topic_app_service = TopicAppService(
        topic_domain_service=TopicDomainService(),
        news_domain_service=NewsDomainService(storage),
    )
    workflow_subscribers = WorkflowEventSubscribers(
        sentiment_app_service=sentiment_app_service,
        topic_app_service=topic_app_service,
    )
    workflow_subscribers.register()

    app.register_blueprint(
        create_external_controller(
            sentiment_app_service=sentiment_app_service,
            topic_app_service=topic_app_service,
        )
    )

    scheduler = Scheduled(
        config_path=config_path,
        storage=storage,
        interval_seconds=get_interval_seconds_from_env(),
        llm_max_workers=llm_max_workers,
        data_fetcher_app_service=data_fetcher_app_service,
        sentiment_app_service=sentiment_app_service,
        topic_app_service=topic_app_service,
    )

    app.config["scheduler"] = scheduler

    @app.get("/health")
    def health() -> Any:
        return jsonify(Result.success_result({"status": "ok"}).to_dict())

app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
