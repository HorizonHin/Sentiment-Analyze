import time
import asyncio
from collections import deque
import logging

logger = logging.getLogger(__name__)

class TokenBucketRateLimiter:
    """
    基于令牌桶算法的速率限制器，支持同步和异步等待。
    用于控制 LLM API 的请求频率。
    """
    def __init__(self, requests_per_minute: int = 60):
        self.capacity = requests_per_minute
        self.tokens = float(requests_per_minute)
        self.fill_rate = requests_per_minute / 60.0  # 每秒补充多少令牌
        self.last_update = time.time()
        self._lock = asyncio.Lock()

    def _add_tokens(self):
        now = time.time()
        delta = now - self.last_update
        self.tokens = min(self.capacity, self.tokens + delta * self.fill_rate)
        self.last_update = now

    async def async_acquire(self):
        """异步获取令牌，如果不足则等待"""
        async with self._lock:
            while True:
                self._add_tokens()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                
                # 计算需要等待的时间
                wait_time = (1.0 - self.tokens) / self.fill_rate
                await asyncio.sleep(wait_time)

    def acquire(self):
        """同步获取令牌，如果不足则由于是简单实现，我们通过 sleep 阻塞"""
        while True:
            self._add_tokens()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            
            wait_time = (1.0 - self.tokens) / self.fill_rate
            time.sleep(wait_time)

class SlidingWindowRateLimiter:
    """
    滑动窗口速率限制器。
    精确限制指定时间窗口内的请求数量。
    """
    def __init__(self, window_seconds: int = 60, max_requests: int = 60):
        self.window_seconds = window_seconds
        self.max_requests = max_requests
        self.requests = deque()
        self._lock = asyncio.Lock()

    async def async_acquire(self):
        async with self._lock:
            while True:
                now = time.time()
                # 移除窗口外的过时请求
                while self.requests and self.requests[0] <= now - self.window_seconds:
                    self.requests.popleft()
                
                if len(self.requests) < self.max_requests:
                    self.requests.append(now)
                    return
                
                # 计算窗口内最早的请求过期的时间
                sleep_time = self.requests[0] + self.window_seconds - now
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

    def acquire(self):
        """同步获取"""
        while True:
            now = time.time()
            # 这里简单处理，假设同步调用不会被频繁并发触发（或调用者已处理并发）
            while self.requests and self.requests[0] <= now - self.window_seconds:
                self.requests.popleft()
            
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return
            
            sleep_time = self.requests[0] + self.window_seconds - now
            if sleep_time > 0:
                time.sleep(sleep_time)
