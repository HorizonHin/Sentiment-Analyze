import json
import os
from typing import Any

from openai import OpenAI


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


class LLMTitleAnalyzer:
    def __init__(
        self,
        model: str = "qwen3.5-flash",
        base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
    ) -> None:
        api_key = os.getenv("Qwen_SentimentAnalyze")
        if not api_key:
            raise ValueError("Missing API key 'Qwen_SentimentAnalyze'.")

        self.model = model
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.prompt_file = os.path.join(os.path.dirname(__file__), "system_prompt.txt")

    def _get_system_prompt(self) -> str:
        with open(self.prompt_file, "r", encoding="utf-8") as f:
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
        event_type = EVENT_TYPE_MAP.get(event_type_raw, "other")

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

    def analyze_title(self, title: str) -> dict[str, Any]:
        if not title or not title.strip():
            raise ValueError("title cannot be empty")

        messages = [
            {"role": "system", "content": self._get_system_prompt()},
            {
                "role": "user",
                "content": (
                    "请分析以下新闻标题并输出 json。仅输出 json，不要输出任何解释。"
                    f"\n标题：{title}"
                ),
            },
        ]

        completion = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.1,
            response_format={"type": "json_object"},
        )

        raw_content = completion.choices[0].message.content or ""
        # print("Raw LLM reply:", raw_content)
        payload = json.loads(self._strip_json_fence(raw_content))
        formatted_result = self._normalize_result(payload)
        return formatted_result

    def analyze_titles(self, titles: list[str]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for title in titles:
            results.append(self.analyze_title(title))
        return results
