from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from flask import Blueprint, jsonify, request

from SentimentAnalyzeServer.application.common import Result
from SentimentAnalyzeServer.application.sentimentAnalyzeAppsService import SentimentAnalyzeAppService
from SentimentAnalyzeServer.application.topicAppService import TopicAppService


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
	if value is None:
		return None

	text = str(value).strip()
	if not text:
		return None

	for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
		try:
			return datetime.strptime(text, fmt)
		except ValueError:
			continue

	try:
		return datetime.fromisoformat(text)
	except ValueError:
		return None


def create_external_controller(
	sentiment_app_service: SentimentAnalyzeAppService,
	topic_app_service: TopicAppService,
) -> Blueprint:
	bp = Blueprint("external_controller", __name__)

	@bp.get("/news/latest-ranked")
	def get_latest_ranked_news() -> object:
		try:
			default_start_time = datetime.now() - timedelta(hours=1)
			default_end_time = datetime.now()
			start_time = _parse_datetime(request.args.get("start_time")) or default_start_time
			end_time = _parse_datetime(request.args.get("end_time")) or default_end_time

			grouped = sentiment_app_service.get_analyzed_news_grouped_by_latest_time(
				start_time=start_time,
				end_time=end_time,
			)
			data = {
				source_id: [item.to_dict() for item in items]
				for source_id, items in grouped.items()
			}
			return jsonify(Result.success_result(data).to_dict())
		except Exception as exc:
			return jsonify(Result.failure_result(str(exc)).to_dict()), 500

	@bp.get("/topics/trending")
	def get_trending_topics() -> object:
		try:
			result = topic_app_service.get_trending_topics()
			if result.success:
				return jsonify(result.to_dict())
			else:
				return jsonify(result.to_dict()), 404
		except Exception as exc:
			return jsonify(Result.failure_result(str(exc)).to_dict()), 500

	return bp
