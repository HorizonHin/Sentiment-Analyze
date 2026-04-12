import threading
import time
import asyncio
import logging
from typing import Optional
import collections
import weakref

logger = logging.getLogger(__name__)

class SlidingWindowRateLimiter:
    def __init__(self, window_seconds: int = 60, max_requests: int = 60):
        self.window_seconds = window_seconds
        self.max_requests = max_requests
        self.requests = collections.deque()
        
        # 使用弱引用字典，自动清理已销毁的 Loop
        self._async_locks = weakref.WeakKeyDictionary()
        self._lock_for_locks = threading.Lock()

    def _get_async_lock(self):
        loop = asyncio.get_running_loop()
        with self._lock_for_locks:
            if loop not in self._async_locks:
                self._async_locks[loop] = asyncio.Lock()
            return self._async_locks[loop]

    async def async_acquire(self):
        async_lock = self._get_async_lock()
        
        while True:
            # 1. 尝试获取名额（尽量缩短线程锁持有时间）
            now = time.time()
            sleep_time = 0
            
            with self._lock_for_locks:
                # 清理过期请求
                while self.requests and self.requests[0] <= now - self.window_seconds:
                    self.requests.popleft()
                
                if len(self.requests) < self.max_requests:
                    self.requests.append(now)
                    return # 成功拿到名额
                
                # 名额满了，计算需要等待多久
                sleep_time = self.requests[0] + self.window_seconds - now

            # 2. 如果没拿到名额，在锁外睡眠
            if sleep_time > 0:
                # 注意：这里不持有任何锁，允许其他协程/线程继续尝试
                await asyncio.sleep(sleep_time)

    def acquire(self):
        """同步获取（保持原有逻辑，增加线程安全）"""
        while True:
            now = time.time()
            with self._lock_for_locks:
                while self.requests and self.requests[0] <= now - self.window_seconds:
                    self.requests.popleft()
                
                if len(self.requests) < self.max_requests:
                    self.requests.append(now)
                    return
                
                sleep_time = self.requests[0] + self.window_seconds - now
            
            if sleep_time > 0:
                time.sleep(sleep_time)