from dataclasses import dataclass
import threading
from typing import Any, Callable, Dict, List


EVENT_CRAWL_SAVED = "crawl.saved"
EVENT_SENTIMENT_ANALYZED = "sentiment.analyzed"

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
            handler(payload)