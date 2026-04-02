# coding=utf-8
"""
Stealth browser bootstrap utilities.
"""

from __future__ import annotations

from typing import Optional, Tuple

from playwright.async_api import Browser, BrowserContext, Playwright, async_playwright


async def create_stealth_browser(
    proxy_url: Optional[str] = None,
) -> Tuple[Playwright, Browser, BrowserContext]:
    """
    创建一个高度伪装的 Playwright 浏览器实例。

    Returns:
        (playwright, browser_instance, context)
    """
    playwright = await async_playwright().start()

    launch_options = {
        "headless": False,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-extensions",
            "--disable-plugins",
            "--lang=zh-CN",
        ],
    }

    browser_instance = await playwright.chromium.launch(**launch_options)

    context_options = {
        "viewport": {"width": 1920, "height": 1080},
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "locale": "zh-CN",
        "timezone_id": "Asia/Shanghai",
        "permissions": ["geolocation"],
        "geolocation": {"latitude": 39.9042, "longitude": 116.4074},
        "color_scheme": "dark",
        "ignore_https_errors": True,
        "bypass_csp": True,
    }

    if proxy_url:
        context_options["proxy"] = {"server": proxy_url}

    context = await browser_instance.new_context(**context_options)

    await context.add_init_script(
        """
        delete navigator.__proto__.webdriver;
        window.chrome = {
            runtime: {},
            loadTimes: () => ({
                firstPaintAfterLoadTime: 0,
                navigationStart: 0,
                wasFetchedViaSpdy: false,
                wasNpnNegotiated: false
            })
        };
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
        );
        """
    )

    return playwright, browser_instance, context
