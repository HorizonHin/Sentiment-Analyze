# coding=utf-8
"""Flask 应用入口，包含新闻平台定时抓取任务。"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from flask import Flask, jsonify
import yaml

_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE_ROOT))

from SentimentAnalyzeServer.application.scheduled_crawler import (
    ScheduledCrawler,
    get_interval_seconds_from_env,
)
from SentimentAnalyzeServer.domain.news.sqlite_backend import SQLiteStorageBackend


def _should_start_scheduler(app: Flask) -> bool:
    if not app.debug:
        return True
    return os.environ.get("WERKZEUG_RUN_MAIN") == "true"


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
    db_path = os.getenv("NEWS_DB_PATH")
    storage = SQLiteStorageBackend(db_path=db_path) if db_path else SQLiteStorageBackend()
    crawler = ScheduledCrawler(
        config_path=config_path,
        storage=storage,
        interval_seconds=get_interval_seconds_from_env(),
        llm_max_workers=llm_max_workers,
    )

    app.config["crawler"] = crawler

    @app.get("/health")
    def health() -> Any:
        return jsonify({"status": "ok"})

    @app.post("/tasks/crawl/run")
    def run_crawl_once() -> Any:
        result = crawler.run_once()
        return jsonify(result)

    if _should_start_scheduler(app):
        crawler.start()

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
