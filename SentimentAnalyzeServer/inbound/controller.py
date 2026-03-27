from __future__ import annotations

import math
from decimal import Decimal
import time
from typing import Any, Optional

import logging

from flask import Blueprint, jsonify, request

from SentimentAnalyzeServer.application.common import Result
from SentimentAnalyzeServer.application.sentimentAnalyzeAppsService import SentimentAnalyzeAppService
from SentimentAnalyzeServer.application.topicAppService import TopicAppService
from SentimentAnalyzeServer.system.datetime_utils import parse_int_timestamp


_LATEST_RANKED_LOOKBACK_MULTIPLIER = 2.2
logger = logging.getLogger(__name__)


def create_external_controller(
	sentiment_app_service: SentimentAnalyzeAppService,
	topic_app_service: TopicAppService,
	crawl_interval_seconds: int,
) -> Blueprint:
	bp = Blueprint("external_controller", __name__)

	@bp.get("/news/latest-ranked")
	def get_latest_ranked_news() -> object:
		try:
			lookback_seconds = max(60, int(crawl_interval_seconds)) * _LATEST_RANKED_LOOKBACK_MULTIPLIER
			default_end_time = int(time.time())
			default_start_time = default_end_time - int(lookback_seconds)
			start_time = parse_int_timestamp(request.args.get("start_time")) or default_start_time
			end_time = parse_int_timestamp(request.args.get("end_time")) or default_end_time

			grouped = sentiment_app_service.get_analyzed_news_grouped_by_latest_time(
				start_time=start_time,
				end_time=end_time,
			)
			data = {
				source_id: [item.to_dict() for item in items]
				for source_id, items in grouped.items()
			}
			return jsonify(Result.success_result(data).to_dict())
		
		except ValueError as exc:
			return jsonify(Result.failure_result(str(exc)).to_dict()), 400
		except Exception as exc:
			logger.exception("Failed to build response for /news/latest-ranked")
			return jsonify(Result.failure_result(str(exc)).to_dict()), 500

	@bp.get("/topics/trending")
	def get_trending_topics() -> object:
		try:
			result = topic_app_service.get_trending_topics()
			if result.success:
				return jsonify(result.to_dict())
			else:
				return jsonify(result.to_dict())
		except Exception as exc:
			print(exc)
			return jsonify(Result.failure_result(str(exc)).to_dict())

	@bp.get("/topics/snapshot-detail")
	def get_topic_snapshot_detail() -> object:
		try:
			created_at_raw = request.args.get("created_at")
			topic_id_raw = request.args.get("id")
			history_limit_raw = request.args.get("history_limit", "100")

			topic_created_at = parse_int_timestamp(created_at_raw)
			if topic_created_at is None:
				return jsonify(Result.failure_result("参数 created_at 无效，必须是秒级 int timestamp").to_dict())

			try:
				topic_id = int(topic_id_raw)
			except (TypeError, ValueError):
				return jsonify(Result.failure_result("参数 id 无效，必须是整数").to_dict())

			try:
				history_limit = max(1, int(history_limit_raw))
			except (TypeError, ValueError):
				history_limit = 100

			result = topic_app_service.get_topic_snapshot_detail(
				topic_created_at=topic_created_at,
				topic_id=topic_id,
				history_limit=history_limit,
			)
		# 	payload = {
        #     "topic": timeline[0].to_dict(),
        #     "timeline": [entry.to_dict() for entry in timeline],
        # }
			if result.success:
				return jsonify(result.to_dict())
			return jsonify(result.to_dict())
		except ValueError as exc:
			return jsonify(Result.failure_result(str(exc)).to_dict())
		except Exception as exc:
			return jsonify(Result.failure_result(str(exc)).to_dict())

	return bp
