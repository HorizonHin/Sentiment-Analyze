import json
import logging
import os
import random
import time
from typing import Any, List

from openai import BadRequestError, OpenAI, RateLimitError
from SentimentAnalyzeServer.system.rate_limiter import SlidingWindowRateLimiter


EVENT_TYPE_MAP = {
    "business_competition": "business_competition",
    "商业竞争": "business_competition",
    "市场竞争": "business_competition",
    "policy_change": "policy_change",
    "政策变化": "policy_change",
    "政策调整": "policy_change",
    "product_launch": "product_launch",
    "产品发布": "product_launch",
    "新品发布": "product_launch",
    "crisis_event": "crisis_event",
    "危机事件": "crisis_event",
    "technology_breakthrough": "technology_breakthrough",
    "技术突破": "technology_breakthrough",
}

POLARITY_SET = {"positive", "negative", "neutral", "mixed"}
ENTITY_TYPE_SET = {"company", "product", "person", "policy", "org", "unknown"}


logger = logging.getLogger(__name__)


class LLMTitleAnalyzer:
    def __init__(
        self,
        model: str = "qwen-turbo-2025-07-15", #qwen3.5-flash
        base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
        api_key: str | None = None,
    ) -> None:
        api_key = api_key or os.getenv("Qwen_SentimentAnalyze")
        if not api_key:
            raise ValueError("Missing API key 'Qwen_SentimentAnalyze'.")

        self.model = model
        self.client = OpenAI(api_key=api_key, base_url=base_url
                            )
        self.prompt_file = os.path.join(os.path.dirname(__file__), "analyze_title_prompt.txt")
        self.topic_prompt_file = os.path.join(os.path.dirname(__file__), "analyze_topic_title.txt")
        self.title_only_prompt_file = os.path.join(os.path.dirname(__file__), "analyze_title_only_prompt.txt")
        self.max_retries = 5
        self.initial_retry_delay = 1.0
        
        # 滑动窗口速率限制：每分钟最大 120 个请求 (针对通义千问免费版/基础版)
        self._rate_limiter = SlidingWindowRateLimiter(window_seconds=60, max_requests=120)

    def _get_analyze_title_prompt(self) -> str:
        with open(self.prompt_file, "r", encoding="utf-8") as f:
            return f.read().strip()

    def _get_analyze_topic_prompt(self) -> str:
        with open(self.topic_prompt_file, "r", encoding="utf-8") as f:
            return f.read().strip()

    def _get_analyze_title_only_prompt(self) -> str:
        with open(self.title_only_prompt_file, "r", encoding="utf-8") as f:
            return f.read().strip()

    @staticmethod
    def _clamp(v: Any, default: float = 0.0) -> float:
        try:
            n = float(v)
        except (TypeError, ValueError):
            n = default
        return max(0.0, min(1.0, n))

    @staticmethod
    def _strip_json_fence(text: str) -> str:
        s = text.strip()
        if s.startswith("```"):
            s = s.strip("`")
            if s.lower().startswith("json"):
                s = s[4:].strip()
        return s

    def _normalize_entities(self, entities: Any) -> list[dict[str, str]]:
        if not isinstance(entities, list):
            return []

        output: list[dict[str, str]] = []
        for item in entities:
            if isinstance(item, dict):
                name = str(item.get("name", "")).strip()
                e_type = str(item.get("type", "unknown")).strip().lower() or "unknown"
            elif isinstance(item, str):
                name = item.strip()
                e_type = "unknown"
            else:
                continue

            if not name:
                continue
            output.append({"name": name, "type": e_type})
        return output

    def _normalize_sentiment(self, sentiment: Any) -> dict[str, Any]:
        if not isinstance(sentiment, dict):
            sentiment = {}

        positive = self._clamp(sentiment.get("positive_ratio", sentiment.get("positive", 0.0)))
        negative = self._clamp(sentiment.get("negative_ratio", sentiment.get("negative", 0.0)))
        neutral = self._clamp(sentiment.get("neutral_ratio", sentiment.get("neutral", 0.0)))

        total = positive + negative + neutral
        if total <= 0:
            positive, negative, neutral = 0.34, 0.33, 0.33
        elif abs(total - 1.0) > 0.01:
            positive, negative, neutral = positive / total, negative / total, neutral / total

        polarity = str(sentiment.get("polarity", "")).lower().strip()
        if polarity not in POLARITY_SET:
            top = max(
                [("positive", positive), ("negative", negative), ("neutral", neutral)],
                key=lambda x: x[1],
            )[0]
            spread = max(positive, negative, neutral) - min(positive, negative, neutral)
            polarity = "mixed" if spread < 0.2 else top

        dimensions = sentiment.get("dimensions")
        if not isinstance(dimensions, dict):
            dimensions = {}

        return {
            "polarity": polarity,
            "positive_ratio": positive,
            "negative_ratio": negative,
            "neutral_ratio": neutral,
            "dimensions": {
                "optimism": self._clamp(dimensions.get("optimism", positive), 0.5),
                "trust": self._clamp(dimensions.get("trust", positive * 0.7 + neutral * 0.3), 0.5),
                "attention": self._clamp(dimensions.get("attention", 0.75), 0.75),
                "controversy": self._clamp(
                    dimensions.get("controversy", 2 * min(positive, negative)),
                    0.4,
                ),
            },
        }

    def _normalize_result(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("LLM payload is not a JSON object")

        event_type_raw = str(payload.get("event_type", "other")).strip()
        event_type = EVENT_TYPE_MAP.get(event_type_raw, event_type_raw)

        summary = str(payload.get("summary", "")).strip()
        if not summary:
            summary = "该标题反映了相关主体舆情动态，后续影响需结合更多上下文信息判断。"

        return {
            "entities": self._normalize_entities(payload.get("entities", [])),
            "event_type": event_type,
            "summary": summary,
            "keywords": payload.get("keywords", []),
            "sentiment_analysis": self._normalize_sentiment(payload.get("sentiment_analysis", {})),
        }

    def _normalize_title_only_result(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("LLM payload is not a JSON object")

        return {
            "entities": self._normalize_entities(payload.get("entities", [])),
            "event_type": "other",  
            "summary": "无该来源评论抓取支持，仅分析标题实体和关键词",
            "keywords": payload.get("keywords", []),
            "sentiment_analysis": self._normalize_sentiment({}),  # Default neutral sentiment
        }

    @staticmethod
    def _extract_error_code(exc: BadRequestError) -> str:
        # Compatible with different OpenAI SDK error payload shapes.
        if getattr(exc, "code", None):
            return str(exc.code)

        body = getattr(exc, "body", None)
        if isinstance(body, dict):
            err = body.get("error", {})
            if isinstance(err, dict) and err.get("code"):
                return str(err.get("code"))
        return ""

    def _build_empty_result(self) -> dict[str, Any]:
        """Return a safe fallback payload that can be persisted by downstream logic."""
        return {
            "entities": [],
            "event_type": "other",
            "summary": "该标题触发内容审核，未返回可用分析结果。",
            "keywords": [],
            "sentiment_analysis": {
                "polarity": "neutral",
                "positive_ratio": 0.0,
                "negative_ratio": 0.0,
                "neutral_ratio": 1.0,
                "dimensions": {
                    "optimism": 0.5,
                    "trust": 0.5,
                    "attention": 0.75,
                    "controversy": 0.0,
                },
            },
        }

    def analyze_title_and_comments(self, title: str, comments: List[str] | None = None) -> dict[str, Any]:
        if not title or not title.strip():
            raise ValueError("title cannot be empty")

        comments_text = "\n".join(comments) if comments else "无"

        for attempt in range(1, self.max_retries + 1):
            try:
                # 获取速率限制令牌
                self._rate_limiter.acquire()

                messages = [
                    {"role": "system", "content": self._get_analyze_title_prompt()},
                    {
                        "role": "user",
                        "content": (
                            "请分析以下新闻标题及公众评论并输出 json。仅输出 json，不要输出任何解释。"
                            f"\n新闻标题：{title}"
                            f"\n公众评论：\n{comments_text}"
                        ),
                    },
                ]

                completion = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0.1,
                    response_format={"type": "json_object"},
                    extra_body={"enable_thinking": False}
                )

                raw_content = completion.choices[0].message.content or ""
                payload = json.loads(self._strip_json_fence(raw_content))
                formatted_result = self._normalize_result(payload)
                return formatted_result
            except BadRequestError as e:
                error_code = self._extract_error_code(e)
                if error_code == "data_inspection_failed":
                    logger.warning(
                        "LLM 内容审核拦截，返回空分析结果。title=%s, code=%s",
                        title,
                        error_code,
                    )
                    return self._build_empty_result()

                logger.exception(
                    "LLM 分析标题 BadRequest，停止重试。title=%s, code=%s",
                    title,
                    error_code,
                )
                raise
            except RateLimitError as e:
                if attempt < self.max_retries:
                    wait_seconds = self.initial_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time.sleep(wait_seconds)
                    continue
                logger.exception("LLM 分析标题连续触发速率限制，已超过重试次数。title=%s", title)
                raise
            except Exception:
                if attempt < self.max_retries:
                    wait_seconds = self.initial_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time.sleep(wait_seconds)
                    continue

                logger.exception("LLM 分析标题异常 (非速率限制)，已超过重试次数。title=%s", title)
                raise

    def analyze_title_only(self, title: str) -> dict[str, Any]:
        if not title or not title.strip():
            raise ValueError("title cannot be empty")

        for attempt in range(1, self.max_retries + 1):
            try:
                # 获取速率限制令牌
                self._rate_limiter.acquire()

                messages = [
                    {"role": "system", "content": self._get_analyze_title_only_prompt()},
                    {
                        "role": "user",
                        "content": (
                            "请分析以下新闻标题的实体和关键词，仅输出 json。\n"
                            f"标题：{title}"
                        ),
                    },
                ]

                completion = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0.1,
                    response_format={"type": "json_object"},
                    extra_body={"enable_thinking": False}
                )

                raw_content = completion.choices[0].message.content or ""
                payload = json.loads(self._strip_json_fence(raw_content))
                return self._normalize_title_only_result(payload)
            except BadRequestError as e:
                error_code = self._extract_error_code(e)
                if error_code == "data_inspection_failed":
                    logger.warning(
                        "LLM 内容审核拦截标题分析，返回空结果。title=%s, code=%s",
                        title, error_code,
                    )
                    return self._build_empty_result()
                logger.exception("LLM 仅分析标题 BadRequest。title=%s, code=%s", title, error_code)
                raise
            except RateLimitError:
                if attempt < self.max_retries:
                    wait_seconds = self.initial_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time.sleep(wait_seconds)
                    continue
                logger.exception("LLM 仅分析标题连续触发速率限制。title=%s", title)
                raise
            except Exception:
                if attempt < self.max_retries:
                    wait_seconds = self.initial_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time.sleep(wait_seconds)
                    continue
                logger.exception("LLM 仅分析标题异常。title=%s", title)
                raise

    def summarize_topic_title(self, old_topic: str, titles: list[str]) -> str:
        cleaned_titles = [str(title).strip() for title in (titles or []) if str(title).strip()]
        if not str(old_topic or "").strip() or not cleaned_titles:
            return ""

        for attempt in range(1, self.max_retries + 1):
            try:
                # 获取速率限制令牌
                self._rate_limiter.acquire()

                title_lines = "\n".join([f"- {title}" for title in cleaned_titles[:50]])
                messages = [
                    {"role": "system", "content": self._get_analyze_topic_prompt()},
                    {
                        "role": "user",
                        "content": (
                            "请判断以下新标题是否仍属于旧话题，仅输出 JSON。"
                            f"\n旧话题：{old_topic}"
                            f"\n新标题列表：\n{title_lines}"
                        ),
                    },
                ]

                completion = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0.1,
                    response_format={"type": "json_object"},
                    extra_body={"enable_thinking": False},
                )

                raw_content = completion.choices[0].message.content or ""
                payload = json.loads(self._strip_json_fence(raw_content))
                if not isinstance(payload, dict):
                    return ""

                llm_title = payload.get("llm_title", "")
                return str(llm_title == "" and old_topic or llm_title).strip()
            except BadRequestError as e:
                error_code = self._extract_error_code(e)
                if error_code == "data_inspection_failed":
                    logger.warning(
                        "LLM 话题标题总结被内容审核拦截，返回空字符串。old_topic=%s",
                        old_topic,
                    )
                    return ""

                logger.exception(
                    "LLM 话题标题总结 BadRequest，停止重试。old_topic=%s, code=%s",
                    old_topic,
                    error_code,
                )
                raise
            except RateLimitError:
                if attempt < self.max_retries:
                    wait_seconds = self.initial_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time.sleep(wait_seconds)
                    continue
                logger.exception("LLM 话题标题总结连续触发速率限制，已超过重试次数。old_topic=%s", old_topic)
                raise
            except Exception:
                if attempt < self.max_retries:
                    wait_seconds = self.initial_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time.sleep(wait_seconds)
                    continue
                logger.exception("LLM 话题标题总结异常，已超过重试次数。old_topic=%s", old_topic)
                raise

