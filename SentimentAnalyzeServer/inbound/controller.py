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
    risk_warning_domain_service: Any,
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

	@bp.post("/topics/batch-get")
	def batch_get_topics() -> object:
		"""批量拉取话题详细信息
		Payload: { "keys": [ [created_at, topic_id], ... ] }
		"""
		try:
			payload = request.get_json(silent=True) or {}
			keys_raw = payload.get("keys", [])
			if not keys_raw:
				return jsonify(Result.failure_result("参数 keys 不能为空且必须是列表").to_dict())
			
			# Ensure it's a list of tuples/lists [int, int]
			keys = []
			for k in keys_raw:
				if isinstance(k, (list, tuple)) and len(k) >= 2:
					try:
						keys.append((int(k[0]), int(k[1])))
					except (TypeError, ValueError):
						continue
			
			if not keys:
				return jsonify(Result.failure_result("未提供有效的 (created_at, topic_id) 组合").to_dict())

			result = topic_app_service.get_topics_by_composite_keys(keys)
			return jsonify(result.to_dict())
		except Exception as exc:
			logger.exception("Failed to execute /topics/batch-get")
			return jsonify(Result.failure_result(str(exc)).to_dict())

	@bp.post("/keywords/followed/add")
	def add_followed_keyword() -> object:
		try:
			payload = request.get_json(silent=True) or {}
			keyword_term = str(payload.get("keyword_term", "")).strip()
			if not keyword_term:
				keyword_term = str(request.args.get("keyword_term", "")).strip()
			if not keyword_term:
				return jsonify(Result.failure_result("参数 keyword_term 不能为空").to_dict())

			added = sentiment_app_service.news_domain_service.add_followed_keyword(keyword_term)
			if not added:
				return jsonify(Result.failure_result("关键词已关注或添加失败").to_dict())

			return jsonify(
				Result.success_result({"keyword_term": keyword_term, "added": True}).to_dict()
			)
		except Exception as exc:
			logger.exception("Failed to execute /keywords/followed/add")
			return jsonify(Result.failure_result(str(exc)).to_dict())

	@bp.delete("/keywords/followed/delete")
	def delete_followed_keyword() -> object:
		try:
			keyword_term = str(request.args.get("keyword_term", "")).strip()
			if not keyword_term:
				payload = request.get_json(silent=True) or {}
				keyword_term = str(payload.get("keyword_term", "")).strip()
			if not keyword_term:
				return jsonify(Result.failure_result("参数 keyword_term 不能为空").to_dict())

			deleted = sentiment_app_service.news_domain_service.delete_followed_keyword(keyword_term)
			if not deleted:
				return jsonify(Result.failure_result("关键词未关注或删除失败").to_dict())

			return jsonify(
				Result.success_result({"keyword_term": keyword_term, "deleted": True}).to_dict()
			)
		except Exception as exc:
			logger.exception("Failed to execute /keywords/followed/delete")
			return jsonify(Result.failure_result(str(exc)).to_dict())

	@bp.get("/keywords/followed/list")
	def list_followed_keywords() -> object:
		try:
			limit_raw = request.args.get("limit")
			try:
				limit = max(1, int(limit_raw)) if limit_raw is not None else 1000
			except (TypeError, ValueError):
				limit = 1000

			keywords = sentiment_app_service.news_domain_service.list_followed_keywords(limit=limit)
			return jsonify(Result.success_result({"keywords": keywords}).to_dict())
		except Exception as exc:
			logger.exception("Failed to execute /keywords/followed/list")
			return jsonify(Result.failure_result(str(exc)).to_dict())

	@bp.get("/risk/topic-warnings")
	def get_topic_risk_warnings() -> object:
		try:
			topic_created_at = parse_int_timestamp(request.args.get("topic_created_at"))
			topic_id_raw = request.args.get("topic_id")
			topic_id = int(topic_id_raw) if topic_id_raw else None
			start_time = parse_int_timestamp(request.args.get("start_time"))
			end_time = parse_int_timestamp(request.args.get("end_time"))
			risk_level = request.args.get("risk_level")
			limit = max(1, int(request.args.get("limit", 100)))

			warnings = risk_warning_domain_service.get_topic_risk_warnings(
				topic_created_at=topic_created_at,
				topic_id=topic_id,
				start_time=start_time,
				end_time=end_time,
				risk_level=risk_level,
				limit=limit,
			)
			data = [
				{
					"topic_created_at": w.topic_created_at,
					"topic_id": w.topic_id,
					"topic_name": w.topic_name,
					"risk_type": w.risk_type,
					"risk_level": w.risk_level,
					"risk_score": w.risk_score,
					"reason": w.reason,
					"metrics": w.metrics,
					"detected_by_event": w.detected_by_event,
					"occurred_at": w.occurred_at,
				}
				for w in warnings
			]
			return jsonify(Result.success_result(data).to_dict())
		except Exception as exc:
			logger.exception("Failed to execute /risk/topic-warnings")
			return jsonify(Result.failure_result(str(exc)).to_dict())

	@bp.get("/risk/sensitive-title-audit")
	def get_sensitive_title_records() -> object:
		try:
			topic_created_at = parse_int_timestamp(request.args.get("topic_created_at"))
			topic_id_raw = request.args.get("topic_id")
			topic_id = int(topic_id_raw) if topic_id_raw else None
			start_time = parse_int_timestamp(request.args.get("start_time"))
			end_time = parse_int_timestamp(request.args.get("end_time"))
			limit = max(1, int(request.args.get("limit", 100)))

			records = risk_warning_domain_service.get_sensitive_title_records(
				topic_created_at=topic_created_at,
				topic_id=topic_id,
				start_time=start_time,
				end_time=end_time,
				limit=limit,
			)
			data = [
				{
					"topic_created_at": r.topic_created_at,
					"topic_id": r.topic_id,
					"topic_name": r.topic_name,
					"old_topic": r.old_topic,
					"candidate_titles": r.candidate_titles,
					"reason": r.reason,
					"risk_level": r.risk_level,
					"occurred_at": r.occurred_at,
					"context": r.context,
				}
				for r in records
			]
			return jsonify(Result.success_result(data).to_dict())
		except Exception as exc:
			logger.exception("Failed to execute /risk/sensitive-title-audit")
			return jsonify(Result.failure_result(str(exc)).to_dict())


	return bp