from dataclasses import dataclass
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
import os
import threading
from typing import Any, Callable, Dict, List, Optional


EVENT_CRAWL_SAVED = "crawl.saved"
EVENT_SENTIMENT_ANALYZED = "sentiment.analyzed"
REDIS_KEY_LATEST_UPDATED_ANALYZED_NEWS = "news:latest_updated_analyzed"
REDIS_KEY_RECENT_30M_ANALYZED_NEWS = "news:recent_30m_analyzed"


logger = logging.getLogger(__name__)


class CommonThreadPool:
    _instance: "CommonThreadPool | None" = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "CommonThreadPool":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    try:
                        max_workers = max(1, int(os.getenv("COMMON_THREAD_POOL_MAX_WORKERS", "8")))
                    except ValueError:
                        max_workers = 8
                    cls._instance._executor = ThreadPoolExecutor(
                        max_workers=max_workers,
                        thread_name_prefix="common-worker",
                    )
        return cls._instance

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any):
        return self._executor.submit(fn, *args, **kwargs)


class MyRedis:
    _instance: "MyRedis | None" = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "MyRedis":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._cache = {}
                    cls._instance._lock = threading.RLock()
        return cls._instance

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        expires_at: Optional[float] = None
        if ttl_seconds is not None and ttl_seconds > 0:
            expires_at = datetime.now().timestamp() + ttl_seconds

        with self._lock:
            self._cache[key] = {
                "value": deepcopy(value),
                "expires_at": expires_at,
            }

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            entry = self._cache.get(key)
            if not entry:
                return default

            expires_at = entry.get("expires_at")
            if expires_at is not None and datetime.now().timestamp() > float(expires_at):
                self._cache.pop(key, None)
                return default

            return deepcopy(entry.get("value", default))

    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                result[key] = value
        return result

    def delete(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)

@dataclass(slots=True)
class Result:
    # 直接定义属性，dataclass 会自动生成 __init__
    success: bool
    data: Any = None
    error_message: str = ""

    @classmethod
    def success_result(cls, data: Any = None) -> 'Result':
        # 使用 cls(…) 而不是 Result(…) 更加符合面向对象习惯（支持继承）
        return cls(success=True, data=data)

    @classmethod
    def failure_result(cls, error_message: str) -> 'Result':
        return cls(success=False, error_message=error_message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "data": self.data,
            "error_message": self.error_message,
        }


class EventManager:
    _instance: "EventManager | None" = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "EventManager":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._subscribers = {}
                    cls._instance._lock = threading.RLock()
                    cls._instance._common_thread_pool = CommonThreadPool()
        return cls._instance

    def subscribe(self, event_name: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        with self._lock:
            handlers = self._subscribers.setdefault(event_name, [])
            if handler not in handlers:
                handlers.append(handler)

    def unsubscribe(self, event_name: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        with self._lock:
            handlers = self._subscribers.get(event_name, [])
            if handler in handlers:
                handlers.remove(handler)

    def publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            handlers: List[Callable[[Dict[str, Any]], None]] = list(self._subscribers.get(event_name, []))

        for handler in handlers:
            self._common_thread_pool.submit(self._run_handler, event_name, handler, payload)

    def _run_handler(
        self,
        event_name: str,
        handler: Callable[[Dict[str, Any]], None],
        payload: Dict[str, Any],
    ) -> None:
        try:
            handler(payload)
        except Exception:
            logger.exception("Event handler failed. event_name=%s, handler=%s", event_name, getattr(handler, "__name__", repr(handler)))