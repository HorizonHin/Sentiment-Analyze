# coding=utf-8
"""
Single-instance async browser client with multi-page concurrency support.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional

from SentimentAnalyzeServer.domain.crawler.stealth_config import create_stealth_browser

logger = logging.getLogger(__name__)


class AsyncBrowserClient:
    """单实例、多页面并发的浏览器客户端。"""

    _instance: Optional["AsyncBrowserClient"] = None
    _instance_lock = asyncio.Lock()

    def __init__(
        self,
        proxy_url: Optional[str],
        headers_config: Dict,
        default_headers: Dict[str, str],
        max_comments: int,
        max_concurrent_pages: int = 5,
    ):
        self.proxy_url = proxy_url
        self.headers_config = headers_config or {}
        self.default_headers = default_headers
        self.max_comments = max_comments

        self._playwright = None
        self._browser = None
        self._shared_context = None
        self._started = False
        self._startup_lock = asyncio.Lock()
        self._page_semaphore = asyncio.Semaphore(max_concurrent_pages)

    @classmethod
    async def get_instance(
        cls,
        proxy_url: Optional[str],
        headers_config: Dict,
        default_headers: Dict[str, str],
        max_comments: int,
        max_concurrent_pages: int = 5,
    ) -> "AsyncBrowserClient":
        async with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(
                    proxy_url=proxy_url,
                    headers_config=headers_config,
                    default_headers=default_headers,
                    max_comments=max_comments,
                    max_concurrent_pages=max_concurrent_pages,
                )
            return cls._instance

    def get_headers(self, method_name: str) -> Dict[str, str]:
        return self.headers_config.get(method_name, self.default_headers).copy()

    async def ensure_started(self):
        if self._started:
            return

        async with self._startup_lock:
            if self._started:
                return
            self._playwright, self._browser, self._shared_context = await create_stealth_browser(
                proxy_url=self.proxy_url
            )
            await self._shared_context.set_extra_http_headers(self.default_headers)
            self._started = True

    async def acquire_page(self):
        await self.ensure_started()
        await self._page_semaphore.acquire()
        page = await self._shared_context.new_page()
        return page

    async def release_page(self, page):
        try:
            await page.close()
        finally:
            self._page_semaphore.release()

    async def new_isolated_context(self, **context_options):
        await self.ensure_started()
        return await self._browser.new_context(**context_options)

    async def close(self):
        try:
            if self._shared_context:
                await self._shared_context.close()
                self._shared_context = None
            if self._browser:
                await self._browser.close()
                self._browser = None
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
            self._started = False
        except Exception as e:
            logger.warning(f"关闭浏览器实例失败: {e}")
