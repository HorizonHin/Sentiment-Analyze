import logging
from queue import Queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, List, Optional

logger = logging.getLogger(__name__)


class QueueBatchManager:
    def __init__(self, batch_size: int, consume_batch: Callable[[List[Any]], None]) -> None:
        self.batch_size = max(1, batch_size)
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


class LLMExecutorService:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(LLMExecutorService, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, max_workers: int = 20):
        if self._initialized:
            return

        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="LLMAnalyzerPool",
        )
        self._initialized = True
        logger.info("LLM Executor Service initialized with %s workers.", max_workers)

    def execute(self, func: Callable, *args, **kwargs) -> Future:
        """
        提交领域服务的分析任务
        :param func: 领域服务中的同步方法 (如 analyzer.analyze_text)
        """
        return self._executor.submit(func, *args, **kwargs)

    def batch_execute(self, func: Callable, data_list: list) -> list:
        """
        批量处理并阻塞等待结果（适用于应用层同步返回场景）
        """
        return list(self._executor.map(func, data_list))

    def get_status(self) -> dict[str, Any]:
        """监控接口：获取当前线程池堆积情况"""
        return {
            "workers": self._executor._max_workers,
            "pending_tasks": self._executor._work_queue.qsize(),
        }

    def create_queue_batch_manager(
        self,
        batch_size: int,
        consume_batch: Callable[[List[Any]], None],
    ) -> QueueBatchManager:
        return QueueBatchManager(batch_size=batch_size, consume_batch=consume_batch)

    def shutdown(self) -> None:
        logger.info("Shutting down LLM Executor...")
        self._executor.shutdown(wait=True)
