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
        self._lock = threading.Lock()

    async def async_acquire(self):
        """异步获取名额"""
        while True:
            now = time.time()
            sleep_time = 0
            
            with self._lock:
                # 清理过期请求
                while self.requests and self.requests[0] <= now - self.window_seconds:
                    self.requests.popleft()
                
                if len(self.requests) < self.max_requests:
                    self.requests.append(now)
                    return # 成功拿到名额
                
                # 名额满了，计算需要等待多久
                sleep_time = self.requests[0] + self.window_seconds - now

            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    def acquire(self):
        """同步获取名额"""
        while True:
            now = time.time()
            with self._lock:
                while self.requests and self.requests[0] <= now - self.window_seconds:
                    self.requests.popleft()
                
                if len(self.requests) < self.max_requests:
                    self.requests.append(now)
                    return
                
                sleep_time = self.requests[0] + self.window_seconds - now
            
            if sleep_time > 0:
                time.sleep(sleep_time)