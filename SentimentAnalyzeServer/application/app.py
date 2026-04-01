# coding=utf-8
"""Flask 应用入口"""

from __future__ import annotations

from logging.handlers import RotatingFileHandler
import sys
import atexit
from pathlib import Path
from typing import Any

from flask import Flask, jsonify
import yaml
from SentimentAnalyzeServer.application.scheduled import Scheduled
from SentimentAnalyzeServer.application.common import Result, get_config, get_interval_seconds
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
import logging
import os

def get_app_logger( log_dir: str = None) -> logging.Logger:
    if log_dir is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(base_dir, 'system', 'logs')
    
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'root_app.log')

    # 1. 获取 Root Logger
    root_logger = logging.getLogger() # 明确获取根
    root_logger.setLevel(logging.INFO)

    # 2. 【关键】清理所有已存在的 Handler，防止重复或冲突
    # 有些库可能在你启动前就偷偷塞了 Handler
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 3. 创建统一的 Formatter
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(name)s] [%(threadName)s]: %(message)s'
    )

    # 4. 配置新的 FileHandler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # 5. 配置新的 StreamHandler (控制台)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 6. 【黑科技】强制开启所有子模块的传播
    # 遍历当前所有已创建的 logger，强制它们把日志交给 Root
    for name in logging.root.manager.loggerDict:
        if name.startswith("SentimentAnalyzeServer"):
            logging.getLogger(name).propagate = True

    return root_logger

def create_app() -> Flask:
    app_logger = get_app_logger()  # 配置 Root Logger，所有模块日志汇总到一起
    app = Flask(__name__)
    config = get_config()
    root_dir = Path(__file__).resolve().parent.parent
    config_path = root_dir / "config.yaml"

    llm_executor_config = config.get("llm_executor") or {}
    try:
        llm_max_workers = max(1, int(llm_executor_config.get("max_workers", 32)))
    except (TypeError, ValueError):
        llm_max_workers = 32

    interval_seconds = get_interval_seconds()

    sentiment_config = config.get("sentiment") or {}
    try:
        first_time_lookback_days = max(1, int(sentiment_config.get("first_time_lookback_days", 7)))
    except (TypeError, ValueError):
        first_time_lookback_days = 7
    first_time_lookback_seconds = first_time_lookback_days * 24 * 60 * 60

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
            first_time_lookback_days=first_time_lookback_days,
        )
    except RuntimeError as e:
        app_logger.error(f"[错误] {e}", file=sys.stderr)
        sys.exit(1)
    
    data_fetcher_app_service = DataFetcherAppService(config_path=config_path, storage=storage)
    topic_repository = SqlServerTopicRepository(
        server=mssql_server,
        database=mssql_database,
        username=mssql_username,
        password=mssql_password,
        driver=mssql_driver,
    )
    llm_title_analyzer = LLMTitleAnalyzer(api_key=llm_api_key)
    sentiment_app_service = SentimentAnalyzeAppService(
        storage=storage,
        analyzer=llm_title_analyzer,
        max_workers=llm_max_workers,
        recent_window_seconds=interval_seconds,
        first_time_lookback_seconds=first_time_lookback_seconds,
    )
    topic_app_service = TopicAppService(
        topic_domain_service=TopicDomainService(
            topic_repository=topic_repository,
            heat_stage_config=topic_heat_stage_config,
        ),
        news_domain_service=NewsDomainService(storage),
        crawl_interval_seconds=interval_seconds,
        topic_config=config.get("topic") or {},
        llm_title_analyzer=llm_title_analyzer,
        first_time_lookback_seconds=first_time_lookback_seconds,
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
            first_time_lookback_seconds=first_time_lookback_seconds,
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
        first_time_lookback_seconds=first_time_lookback_seconds,
    )

    app.config["scheduler"] = scheduler
    
    # 只有在非 Debug 模式，或者是 Debug 模式下的工作子进程中，才启动调度器
    is_main_process = os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    is_debug_disabled = not app.debug

    if is_main_process or is_debug_disabled:
        # app.config["scheduler"] = scheduler
        # scheduler.start()
        app_logger.info("[Scheduler] 确认在工作进程中启动")
    else:
        app_logger.info("[Scheduler] 检测到 Flask 热重载父进程，跳过启动以防重复")

    def _shutdown_scheduler() -> None:
        scheduler.stop()

    atexit.register(_shutdown_scheduler)

    @app.get("/test")
    def health() -> Any:
        result = data_fetcher_app_service.crawl_and_save_news_data()
        return jsonify(Result.success_result(result).to_dict())
    
    
    
    return app

    

app = create_app()


