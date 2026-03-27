import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, List, Optional

from SentimentAnalyzeServer.system.infra import CommonThreadPool, QueueBatchManager

logger = logging.getLogger(__name__)


class LLMExecutorService:
    _instance: Optional["LLMExecutorService"] = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super(LLMExecutorService, cls).__new__(cls)
                    cls._instance._initialized = False
                    cls._instance._max_workers = None
        return cls._instance

    def __init__(self, max_workers: int = 20):
        if self._initialized:
            if self._max_workers != max_workers:
                logger.info(
                    "LLM Executor Service already initialized with %s workers; ignore new value %s.",
                    self._max_workers,
                    max_workers,
                )
            return

        self._max_workers = max(1, int(max_workers))
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix="LLMAnalyzerPool",
        )
        self._initialized = True
        logger.info("LLM Executor Service initialized with %s workers.", self._max_workers)

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
        return CommonThreadPool.create_queue_batch_manager(
            batch_size=batch_size,
            consume_batch=consume_batch,
        )

    def shutdown(self) -> None:
        logger.info("Shutting down LLM Executor...")
        self._executor.shutdown(wait=True)
