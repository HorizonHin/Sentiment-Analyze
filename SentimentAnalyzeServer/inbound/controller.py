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
			grouped = sentiment_app_service.get_latest_analyzed_news_batch_grouped()
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

	@bp.get("/news/search-terms")
	def search_terms_by_keyword() -> object:
		"""
		参数:
		- keyword: str (必填，搜索词，如: 小米)
		- start_time: int (秒级时间戳)
		- end_time: int (秒级时间戳)
		- news_first_time: int (可选，分区下界)
		- limit: int (可选，默认100)
		"""
		try:
			keyword = str(request.args.get("keyword", "")).strip()
			if not keyword:
				return jsonify(Result.failure_result("参数 keyword 不能为空").to_dict())

			start_time = parse_int_timestamp(request.args.get("start_time"))
			end_time = parse_int_timestamp(request.args.get("end_time"))
			news_first_time = parse_int_timestamp(request.args.get("news_first_time"))
			limit_raw = request.args.get("limit")
			try:
				limit = max(1, int(limit_raw)) if limit_raw is not None else 100
			except (TypeError, ValueError):
				limit = 100

			if start_time is None or end_time is None:
				return jsonify(Result.failure_result("参数 start_time 和 end_time 必须为有效的秒级时间戳").to_dict())

			keywords = topic_app_service.search_keywords_by_query(
				query=keyword,
				start_time=start_time,
				end_time=end_time,
				news_first_time=news_first_time,
				limit=limit,
			)
			entities = topic_app_service.search_entities_by_query(
				query=keyword,
				start_time=start_time,
				end_time=end_time,
				news_first_time=news_first_time,
				limit=limit,
			)

			def key_or_enti_to_dict(obj):
				return {slot: getattr(obj, slot) for slot in obj.__slots__ if hasattr(obj, slot)}

			data = {
				"keyword": keyword,
				"keywords": [key_or_enti_to_dict(kw) for kw in keywords],
				"entities": [key_or_enti_to_dict(e) for e in entities],
			}
			return jsonify(Result.success_result(data).to_dict())
		except Exception as exc:
			logger.exception("Failed to execute /news/search-terms")
			return jsonify(Result.failure_result(str(exc)).to_dict())

	@bp.get("/topics/by-keyword")
	def get_topic_by_keyword() -> object:
		try:
			keyword = str(request.args.get("keyword", "")).strip()
			if not keyword:
				return jsonify(Result.failure_result("参数 keyword 不能为空").to_dict())

			news_first_time = parse_int_timestamp(request.args.get("news_first_time"))
			start_time = parse_int_timestamp(request.args.get("start_time"))
			end_time = parse_int_timestamp(request.args.get("end_time"))

			topic = topic_app_service.get_topic_by_keyword_or_build(
				keyword=keyword,
				news_first_time=news_first_time,
				start_time=start_time,
				end_time=end_time,
			)
			if topic is None:
				return jsonify(Result.failure_result("未找到可返回或可构建的 Topic").to_dict())

			return jsonify(Result.success_result(topic.to_dict()).to_dict())
		except Exception as exc:
			logger.exception("Failed to execute /topics/by-keyword")
			return jsonify(Result.failure_result(str(exc)).to_dict())
		

	return bp