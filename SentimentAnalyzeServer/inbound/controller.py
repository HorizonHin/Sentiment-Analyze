from __future__ import annotations

import math
from decimal import Decimal
import time
from typing import Any, Optional

import logging
from unittest import result

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
	first_time_lookback_seconds: int,
) -> Blueprint:
	bp = Blueprint("external_controller", __name__)

	@bp.get("/news/latest-ranked")
	def get_latest_ranked_news() -> object:
		try:
			lookback_seconds = max(60, int(crawl_interval_seconds)) * _LATEST_RANKED_LOOKBACK_MULTIPLIER
			default_end_time = int(time.time())
			default_start_time = default_end_time - int(lookback_seconds*1.3)
			default_first_time = default_end_time - max(60, int(first_time_lookback_seconds))
			first_time = parse_int_timestamp(request.args.get("first_time")) or default_first_time
			start_time = parse_int_timestamp(request.args.get("start_time")) or default_start_time
			end_time = parse_int_timestamp(request.args.get("end_time")) or default_end_time

			grouped = sentiment_app_service.get_analyzed_news_grouped_by_latest_time(
				first_time=first_time,
				start_time=start_time,
				end_time=end_time,
			)
			data = {
				source_id: [item.to_dict() for item in items]
				for source_id, items in grouped.items()
			}
			return jsonify(Result.success_result(data).to_dict())
		
		except ValueError as exc:
			return jsonify(Result.failure_result(str(exc)).to_dict())
		except Exception as exc:
			logger.exception("Failed to build response for /news/latest-ranked")
			return jsonify(Result.failure_result(str(exc)).to_dict())

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

	@bp.route("/topic/backfill-llm-title", methods=["POST", "GET"])
	def backfill_topic_llm_title() -> object:
		try:
			raw_limit = request.args.get("limit")
			if raw_limit is None:
				payload = request.get_json(silent=True) or {}
				raw_limit = payload.get("limit") if isinstance(payload, dict) else None

			try:
				limit = max(1, min(200, int(raw_limit))) if raw_limit is not None else 50
			except (TypeError, ValueError):
				return jsonify(Result.failure_result("参数 limit 无效，必须是 1-200 的整数").to_dict())

			result = topic_app_service.backfill_missing_llm_titles(limit=limit)
			if bool(result.get("success", False)):
				return jsonify(Result.success_result(result).to_dict())
			return jsonify(Result.failure_result(str(result.get("reason", "backfill_failed"))).to_dict())
		except Exception as exc:
			logger.exception("Failed to execute /topic/backfill-llm-title")
			return jsonify(Result.failure_result(str(exc)).to_dict())

	@bp.get("/news/recommend-hot-terms")
	def recommend_hot_terms_by_time_range() -> object:
		"""
		参数:
		- start_time: int (秒级时间戳)
		- end_time: int (秒级时间戳)
		- news_first_time: int (可选，分区下界)
		- top_n: int (可选，默认100)
		"""
		try:
			start_time = parse_int_timestamp(request.args.get("start_time"))
			end_time = parse_int_timestamp(request.args.get("end_time"))
			news_first_time = parse_int_timestamp(request.args.get("news_first_time"))
			top_n_raw = request.args.get("top_n")
			try:
				top_n = max(1, int(top_n_raw)) if top_n_raw is not None else 100
			except (TypeError, ValueError):
				top_n = 100

			if start_time is None or end_time is None:
				return jsonify(Result.failure_result("参数 start_time 和 end_time 必须为有效的秒级时间戳").to_dict())

			# 直接用 sentiment_app_service.news_domain_service
			news_domain_service = sentiment_app_service.news_domain_service
			kw_groups, entity_groups = news_domain_service.recommend_hot_terms_by_time_range(
				start_time=start_time,
				end_time=end_time,
				news_first_time=news_first_time,
				top_n=top_n,
			)
			# 返回格式：{"keywords": {...}, "entities": {...}}
			def key_or_enti_to_dict(obj):
				return {slot: getattr(obj, slot) for slot in obj.__slots__ if hasattr(obj, slot)}

			data = {
				"keywords": {k: [key_or_enti_to_dict(kw) for kw in v] for k, v in kw_groups.items()},
				"entities": {k: [key_or_enti_to_dict(e) for e in v] for k, v in entity_groups.items()},
			}
			return jsonify(Result.success_result(data).to_dict())
		except Exception as exc:
			logger.exception("Failed to execute /news/recommend-hot-terms")
			return jsonify(Result.failure_result(str(exc)).to_dict())
		
	@bp.get("/recommend_topics")
	def recommend_topics() -> object:
		"""推荐话题接口"""
		try:
			lookback_seconds = crawl_interval_seconds * _LATEST_RANKED_LOOKBACK_MULTIPLIER
			end_time = int(time.time())
			start_time = end_time - int(lookback_seconds)

			result = topic_app_service.recommend_and_cache_topics(
				start_time=start_time,
				end_time=end_time,
			)

			return jsonify(Result.success_result(result).to_dict())
		except Exception as exc:
			logger.exception("recommend_and_cache_topics failed")
			return jsonify(Result.failure_result(str(exc)).to_dict())


	return bp