from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from functools import wraps
import logging
from queue import Queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set

EVENT_CRAWL_SAVED = "crawl.saved"
EVENT_SENTIMENT_ANALYZED = "sentiment.analyzed"
EVENT_TOPIC_RANK_UPDATED = "topic.rank_updated"
REDIS_KEY_LATEST_NOT_NEED_ANALYSIS_NEWS = "news:latest_not_need_analysis"
REDIS_KEY_RECENT_30M_ANALYZED_NEWS = "news:recent_30m_analyzed"


logger = logging.getLogger(__name__)


class QueueBatchManager:
    def __init__(self, batch_size: int, consume_batch: Callable[[List[Any]], None]) -> None:
        self.batch_size = max(1, int(batch_size))
        self.consume_batch = consume_batch
        self._queue: Queue[Optional[Any]] = Queue()
        self._error: Optional[Exception] = None
        self._error_lock = threading.Lock()
        self._consumer = threading.Thread(target=self._consume_loop, daemon=True)
        self._consumer.start()

    def put(self, item: Any) -> None:
        self._queue.put(item)

    def close_and_wait(self) -> None:
        self._queue.put(None)
        self._consumer.join()
        if self._error is not None:
            raise RuntimeError("Queue batch consumer failed.") from self._error

    def _consume_loop(self) -> None:
        try:
            batch: List[Any] = []
            while True:
                item = self._queue.get()
                if item is None:
                    if batch:
                        self.consume_batch(batch)
                    break

                batch.append(item)
                if len(batch) >= self.batch_size:
                    self.consume_batch(batch)
                    batch = []
        except Exception as e:
            with self._error_lock:
                self._error = e
            logger.exception("QueueBatchManager consumer loop failed.")


class CommonThreadPool:
    _instance: "CommonThreadPool | None" = None
    _instance_lock = threading.Lock()
    _configured_max_workers = 8
# 新增：用於追蹤正在運行的任務 ID
    _running_tasks: Set[str] = set()
    _tasks_lock = threading.Lock()

    @classmethod
    def configure(cls, max_workers: int) -> None:
        cls._configured_max_workers = max(1, int(max_workers))

    def __new__(cls) -> "CommonThreadPool":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._executor = ThreadPoolExecutor(
                        max_workers=cls._configured_max_workers,
                        thread_name_prefix="common-worker",
                    )
        return cls._instance

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any):
        return self._executor.submit(fn, *args, **kwargs)
# 新增：嘗試佔用任務位
    def try_acquire_task(self, task_id: str) -> bool:
        with self._tasks_lock:
            if task_id in self._running_tasks:
                return False
            self._running_tasks.add(task_id)
            return True

    # 新增：釋放任務位
    def release_task(self, task_id: str):
        with self._tasks_lock:
            self._running_tasks.discard(task_id)

    @staticmethod
    def create_queue_batch_manager(
        batch_size: int,
        consume_batch: Callable[[List[Any]], None],
    ) -> QueueBatchManager:
        return QueueBatchManager(batch_size=batch_size, consume_batch=consume_batch)

def singleton_task(task_id_provider: Callable[..., str] | None = None):
    """
    防止同名任務在線程池中併發執行的裝飾器。
    :param task_id_provider: 可選，一個函數用於根據原函數參數生成唯一 ID。
                             如果不傳，默認使用函數名。
    """
    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            tp = CommonThreadPool()
            
            # 生成唯一的 Task ID
            if task_id_provider:
                tid = task_id_provider(*args, **kwargs)
            else:
                # 默認 ID：函數名
                tid = f"{fn.__name__}"

            # 嘗試獲取執行權
            if tp.try_acquire_task(tid):
                def task_with_cleanup():
                    try:
                        return fn(*args, **kwargs)
                    finally:
                        # 無論成功失敗，執行完必須釋放
                        tp.release_task(tid)
                
                # 提交到線程池異步執行
                logger.info(f"[ThreadPool] 提交任務: {tid}")
                return tp.submit(task_with_cleanup)
            else:
                logger.info(f"[ThreadPool] 任務 {tid} 正在運行中，跳過本次提交")
                return None
        return wrapper
    return decorator

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

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None, nx: bool = False) -> bool:
        expires_at: Optional[float] = None
        if ttl_seconds is not None and ttl_seconds > 0:
            expires_at = time.time() + ttl_seconds

        try:
            with self._lock:
                # 1. 检查是否存在（且处理过期）
                current_val = self.get(key) # get 方法内部已经处理了过期逻辑
                if nx and current_val is not None:
                    logger.info(f"Redis set NX failed: key '{key}' already exists")
                    return False
                self._cache[key] = {
                    "value": deepcopy(value),
                    "expires_at": expires_at,
                }
            return True
        except Exception as e:
            logger.error(f"Redis set error: {e}")
            return False

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            entry = self._cache.get(key)
            if not entry:
                return default

            expires_at = entry.get("expires_at")
            if expires_at is not None and time.time() > float(expires_at):
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

    def subscribe(self, event_name: str, handler: Callable) -> None:
        with self._lock:
            if event_name not in self._subscribers:
                self._subscribers[event_name] = []
            
            handlers = self._subscribers[event_name]
            
            # 改进：直接比较函数对象本身，或者使用更稳定的 qualname
            # handler.__qualname__ 会包含类名，如 "WorkflowEventSubscribers._on_sentiment_analyzed"
            handler_id = f"{handler.__module__}.{handler.__qualname__}"
            
            exists = any(f"{h.__module__}.{h.__qualname__}" == handler_id for h in handlers)
            
            if not exists:
                handlers.append(handler)
                logger.info(f"[EventManager] Subscribed: {event_name} -> {handler_id}")
            else:
                # 这种日志能帮你快速发现代码里哪里写重了
                logger.debug(f"[EventManager] Skip duplicate subscription: {handler_id}")

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
