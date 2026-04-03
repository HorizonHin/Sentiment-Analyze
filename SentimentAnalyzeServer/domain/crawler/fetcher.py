# coding=utf-8
"""
数据获取器模块

负责从 NewsNow API 抓取新闻数据，支持：
- 单个平台数据获取
- 批量平台数据爬取
- 自动重试机制
- 代理支持
"""

import json
import logging
import random
import re
import time
import os
import yaml
import asyncio
from typing import Dict, List, Tuple, Optional, Union
import requests
from bs4 import BeautifulSoup
from SentimentAnalyzeServer.domain.crawler.async_browser_client import AsyncBrowserClient

logger = logging.getLogger(__name__)

class DataFetcher:
    """数据获取器"""

    # 默认 API 地址
    DEFAULT_API_URL = "https://newsnow.busiyi.world/api/s"
    
    # 支持抓取评论的平台集合。"bilibili","douyin" , "weibo"评论反爬较强，暂不支持启动。
    SUPPORTED_COMMENT_PLATFORMS = {"baidu", "toutiao","douyin" , "bilibili","weibo",
                                   "zhihu", "tieba", "thepaper", "hupu", "tencent-hot"}

    # 默认请求头
    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    }

    def __init__(
        self,
        proxy_url: Optional[str] = None,
        api_url: Optional[str] = None,
        max_comments: int = 30,
    ):
        """
        初始化数据获取器

        Args:
            proxy_url: 代理服务器 URL（可选）
            api_url: API 基础 URL（可选，默认使用 DEFAULT_API_URL）
            max_comments: 最大抓取评论数量（默认 10）
        """
        self.proxy_url = proxy_url
        self.api_url = api_url or self.DEFAULT_API_URL
        self.max_comments = max_comments
        self._browser_clients_by_loop: Dict[int, AsyncBrowserClient] = {}
        
        # 确保截图保存目录存在
        self.screenshot_dir = "screenshots"
        if not os.path.exists(self.screenshot_dir):
            os.makedirs(self.screenshot_dir)
            logger.info(f"创建截图目录: {self.screenshot_dir}")

        self.headers_config = self._load_headers_config()
        # 记录各平台连续抓取到 0 条评论的次数
        self._consecutive_empty_counts: Dict[str, int] = {}
        # 熔断阈值：连续 N 次抓到 0 条则暂时封禁该平台的一个爬取周期（或本次运行）
        self._circuit_break_threshold = 7

    def _load_headers_config(self) -> Dict:
        """加载 headers.yaml 配置文件"""
        config_path = os.path.join(os.path.dirname(__file__), "headers.yaml")
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f) or {}
            except Exception as e:
                logger.error(f"加载 headers.yaml 失败: {e}")
        return {}

    async def _get_browser_client(self) -> AsyncBrowserClient:
        loop = asyncio.get_running_loop()
        loop_key = id(loop)

        if loop_key not in self._browser_clients_by_loop:
            self._browser_clients_by_loop[loop_key] = await AsyncBrowserClient.get_instance(
                proxy_url=self.proxy_url,
                headers_config=self.headers_config,
                default_headers=self.DEFAULT_HEADERS,
                max_comments=self.max_comments,
                max_concurrent_pages=5,
            )
        return self._browser_clients_by_loop[loop_key]

    async def close(self):
        """
        关闭异步浏览器客户端 (异步)
        """
        if not self._browser_clients_by_loop:
            return

        loop = asyncio.get_running_loop()
        loop_key = id(loop)
        client = self._browser_clients_by_loop.pop(loop_key, None)
        if client:
            await client.close()

    async def _wait_locator_visible(self, page, selector: str, timeout: int = 10000) -> bool:
        """Wait for the first matching locator to become visible."""
        locator = page.locator(selector).first
        try:
            await locator.wait_for(state="visible", timeout=timeout)
            return True
        except Exception:
            return False

    def _append_comment(self, comments: List[str], comment: str) -> bool:
        comment = re.sub(r"\s+", " ", str(comment).strip())
        if not comment or comment in comments:
            return len(comments) >= self.max_comments

        comments.append(comment)
        return len(comments) >= self.max_comments

    def fetch_data(
        self,
        id_info: Union[str, Tuple[str, str]],
        max_retries: int = 2,
        min_retry_wait: int = 3,
        max_retry_wait: int = 5,
    ) -> Tuple[Optional[str], str, str]:
        """
        获取指定ID数据，支持重试

        Args:
            id_info: 平台ID 或 (平台ID, 别名) 元组
            max_retries: 最大重试次数
            min_retry_wait: 最小重试等待时间（秒）
            max_retry_wait: 最大重试等待时间（秒）

        Returns:
            (响应文本, 平台ID, 别名) 元组，失败时响应文本为 None
        """
        if isinstance(id_info, tuple):
            id_value, alias = id_info
        else:
            id_value = id_info
            alias = id_value

        url = f"{self.api_url}?id={id_value}&latest"

        proxies = None
        if self.proxy_url:
            proxies = {"http": self.proxy_url, "https": self.proxy_url}

        retries = 0
        while retries <= max_retries:
            try:
                response = requests.get(
                    url,
                    proxies=proxies,
                    headers=self.DEFAULT_HEADERS,
                    timeout=10,
                )
                response.raise_for_status()

                data_text = response.text
                data_json = json.loads(data_text)

                status = data_json.get("status", "未知")
                if status not in ["success", "cache"]:
                    raise ValueError(f"响应状态异常: {status}")

                status_info = "最新数据" if status == "success" else "缓存数据" 
                logger.info(f"获取 {id_value} 成功（{status_info}）")
                return data_text, id_value, alias

            except Exception as e:
                retries += 1
                if retries <= max_retries:
                    base_wait = random.uniform(min_retry_wait, max_retry_wait)  
                    additional_wait = (retries - 1) * random.uniform(1, 2)      
                    wait_time = base_wait + additional_wait
                    logger.warning(f"请求 {id_value} 失败: {e}. {wait_time:.2f} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"请求 {id_value} 失败: {e}")
                    return None, id_value, alias

        return None, id_value, alias

    def crawl_websites(
        self,
        ids_list: List[Union[str, Tuple[str, str]]],
        request_interval: int = 100,
    ) -> Tuple[Dict, Dict, List]:
        """
        爬取多个网站数据

        Args:
            ids_list: 平台ID列表，每个元素可以是字符串 or (平台ID, 别名) 元组    
            request_interval: 请求间隔（毫秒）

        Returns:
            (结果字典, ID到名称的映射, 失败ID列表) 元组
        """
        results = {}
        id_to_name = {}
        failed_ids = []

        for i, id_info in enumerate(ids_list):
            if isinstance(id_info, tuple):
                id_value, name = id_info
            else:
                id_value = id_info
                name = id_value

            id_to_name[id_value] = name
            response, _, _ = self.fetch_data(id_info)

            if response:
                try:
                    data = json.loads(response)
                    results[id_value] = {}

                    for index, item in enumerate(data.get("items", []), 1):     
                        title = item.get("title")
                        # 跳过无效标题（None、float、空字符串）
                        if title is None or isinstance(title, float) or not str(title).strip():
                            continue
                        title = str(title).strip()
                        url = item.get("url", "")
                        mobile_url = item.get("mobileUrl", "")

                        if title in results[id_value]:
                            results[id_value][title]["ranks"].append(index)     
                        else:
                            results[id_value][title] = {
                                "ranks": [index],
                                "url": url,
                                "mobileUrl": mobile_url,
                            }
                except json.JSONDecodeError:
                    logger.error(f"解析 {id_value} 响应失败")
                    failed_ids.append(id_value)
                except Exception as e:
                    logger.error(f"处理 {id_value} 数据出错: {e}")
                    failed_ids.append(id_value)
            else:
                failed_ids.append(id_value)

            # 请求间隔（除了最后一个）
            if i < len(ids_list) - 1:
                actual_interval = request_interval + random.randint(-10, 20)    
                actual_interval = max(50, actual_interval)
                time.sleep(actual_interval / 1000)

        logger.info(f"成功: {list(results.keys())}, 失败: {failed_ids}")        
        return results, id_to_name, failed_ids

    async def crawl_comments_dispatch(self, source_id: str, title: str, url: str) -> List[str]:
        """
        根据 source_id 分发到不同的评论爬取方法 (异步)
        """
        platform_key = source_id.lower()
        
        # 检查熔断状态
        if self._consecutive_empty_counts.get(platform_key, 0) >= self._circuit_break_threshold:
            logger.warning(f"熔断机制触发：平台 {source_id} 最近连续 {self._consecutive_empty_counts[platform_key]} 次未抓取到评论，本次跳过执行。")
            return []

        comments = []
        if "baidu" in platform_key:
            comments = await self.crawl_baidu_comments_opyimized(title, url)
        elif "weibo" in platform_key:
            comments = await self.crawl_weibo_comments(title)
        elif "bilibili" in platform_key:
            comments = await self.crawl_bilibili_comments_optimized(title, url)  
        elif "douyin" in platform_key:
            comments = await self.crawl_douyin_comments(title, url)                  
        elif "toutiao" in platform_key:
            comments = await self.crawl_toutiao_comments(title, url)
        elif "zhihu" in platform_key:
            comments = await self.crawl_zhihu_comments(title, url)
        elif "tieba" in platform_key:
            comments = await self.crawl_tieba_comments(title, url)
        elif "thepaper" in platform_key or "pengpai" in platform_key:
            comments = await self.crawl_thepaper_comments(title, url)
        elif "hupu" in platform_key:
            comments = await self.crawl_hupu_comments(title, url)
        elif "tencent-hot" in platform_key:
            comments = await self.crawl_tencent_hot_comments(title, url)
        else:
            logger.warning(f" {source_id} 暂不支持抓取评论")
            return []

        # 更新连续为空的计数器
        if not comments:
            self._consecutive_empty_counts[platform_key] = self._consecutive_empty_counts.get(platform_key, 0) + 1
            if self._consecutive_empty_counts[platform_key] >= self._circuit_break_threshold:
                logger.error(f"严重警告：平台 {source_id} 已连续 {self._consecutive_empty_counts[platform_key]} 次抓取失败，已进入熔断状态。")
        else:
            # 只要抓到一次有效评论，就重置计数器
            self._consecutive_empty_counts[platform_key] = 0
            
        return comments
           
    async def crawl_baidu_comments_opyimized(self, title: str, url: str) -> List[str]:
        search_url = url
        comments: List[str] = []
        page = None

        try:
            await asyncio.sleep(random.uniform(2.5, 3.5))

            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()
            
            # 1. 使用 Playwright 访问百度搜索列表页
            await page.goto(search_url, wait_until="load", timeout=25*1000)
            
            # 2. 定位首条搜索结果链接
            link_selector = 'div[class*="title_1WDM0"] a'
            first_link_handle = await page.query_selector(link_selector)
            
            if not first_link_handle:
                logger.warning(f"Playwright 无法定位到百度搜索首条链接: {title}")
                if page:
                    await page.screenshot(path=f"{self.screenshot_dir}/baidu_no_link_{int(time.time())}.png")
                return []

            href = await first_link_handle.get_attribute("href")
            if not href:
                logger.warning("Playwright 抓取到空 href")
                return []
            
            if href.startswith("/s?"):
                href = f"https://www.baidu.com{href}"

            # 3. 跳转到详细页
            await page.goto(href, wait_until="domcontentloaded", timeout=20000)

            comment_selector = 'span[class*="type-text"]'
            await page.wait_for_selector(comment_selector, timeout=10000)
            comment_elements = await page.query_selector_all(comment_selector)
            for el in comment_elements:
                text = (await el.inner_text()).strip()
                if text:
                    if self._append_comment(comments, text):
                        return comments[: self.max_comments]
                if len(comments) >= self.max_comments:
                    break

            if not comments:
                logger.info(f"Playwright 为标题 '{title}' 抓取到 0 条百度评论")
        except Exception as e:
            logger.warning(f"crawl_baidu_comments_opyimized 整体失败: {e}")
            if page:
                try:
                    await page.screenshot(path=f"{self.screenshot_dir}/baidu_fatal_{int(time.time())}.png")
                except:
                    pass
            return []
        finally:
            if page:
                browser_client = await self._get_browser_client()
                await browser_client.release_page(page)
        return comments
       
    # 反爬虫机制较强的平台，完全使用 Playwright。
    async def crawl_weibo_comments(self, title: str) -> List[str]:
        comments: List[str] = []
        context = None
        page = None
        await asyncio.sleep(random.uniform(1.0, 3.0))  # 抓取前随机等待，模拟人类行为
        try:
            browser_client = await self._get_browser_client()
            mobile_ua = (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 "
                "Mobile/15E148 Safari/604.1"
            )
            context = await browser_client.new_isolated_context(user_agent=mobile_ua)
            await context.set_extra_http_headers(browser_client.get_headers("crawl_weibo_comments"))
            page = await context.new_page()

            await page.goto("https://m.weibo.cn", wait_until="domcontentloaded", timeout=15000)
            search_type = 60
            search_url = (
                "https://m.weibo.cn/api/container/getIndex?containerid=100103type="
                f"{search_type}&q={requests.utils.quote(title)}&page_type=searchall"
            )
            await page.goto(search_url, wait_until="domcontentloaded", timeout=10000)

            json_text = await page.evaluate("() => document.body.innerText")
            search_data = json.loads(json_text)

            cards = search_data.get("data", {}).get("cards", [])
            mid_list = []
            for card in cards:
                if card.get("card_type") == 11:
                    for item in card.get("card_group", []):
                        if item.get("mblog"):
                            mid_list.append(item.get("mblog", {}).get("id"))
                elif card.get("card_type") == 9 and card.get("mblog"):
                    mid_list.append(card.get("mblog", {}).get("id"))
                if len(mid_list) >= self.max_comments:
                    break

            if not mid_list:
                logger.warning(f"未找到任何微博 mid: {title}")
                return []

            for mid in mid_list:
                try:
                    comments_url = f"https://m.weibo.cn/comments/hotflow?id={mid}&mid={mid}&max_id_type=0"
                    await page.goto(comments_url, wait_until="networkidle", timeout=10000)
                    comments_json_text = await page.evaluate("() => document.body.innerText")
                    if not comments_json_text or not comments_json_text.strip().startswith("{"):
                        continue

                    comments_data = json.loads(comments_json_text)
                    raw_comments = comments_data.get("data", {}).get("data", [])
                    for c in raw_comments:
                        text = c.get("text", "")
                        clean_text = re.sub(r"<[^>]+>", "", text).strip()
                        if self._append_comment(comments, clean_text):
                            break
                    if len(comments) >= self.max_comments:
                        break
                    await asyncio.sleep(random.uniform(1.5, 3.0))  # 每条评论间随机等待，模拟人类行为
                except Exception as loop_e:
                    logger.warning(f"获取 mid={mid} 的评论时出错: {loop_e}")
                    continue

            if not comments:
                logger.info(f"Playwright 为标题 '{title}' 抓取到 0 条微博评论")
        except Exception as e:
            logger.warning(f"crawl_weibo_comments 整体失败: {e}")
            return []
        finally:
            if page:
                await page.close()
            if context:
                await context.close()
        return comments

    # 风控频繁，先停用。监听response的方式比直接在页面上定位评论元素更稳定
    async def crawl_bilibili_comments_optimized(self, title: str, url: str) -> List[str]:
        comments: List[str] = []
        video_urls: List[str] = []
        page = None

        search_url = url or f"https://search.bilibili.com/all?keyword={requests.utils.quote(title)}"
        try:
            await asyncio.sleep(random.uniform(2.0, 3.0))
            proxies = {"http": self.proxy_url, "https": self.proxy_url} if self.proxy_url else None
            response = requests.get(search_url, headers=self.DEFAULT_HEADERS, proxies=proxies, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a.get("href")
                if "/video/BV" in href:
                    resolved = "https:" + href if href.startswith("//") else href
                    clean_url = resolved.split("?")[0]
                    if clean_url not in video_urls:
                        video_urls.append(clean_url)

            if not video_urls:
                logger.warning(f"BeautifulSoup 无法找到 Bilibili 搜索结果: {title}")
                return []
        except Exception as e:
            logger.warning(f"crawl_bilibili_comments_optimized 搜索阶段失败: {e}")
            return []

        try:
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()

            for video_url in video_urls:
                await asyncio.sleep(random.uniform(2.0, 4.0))  # 视频间随机等待，模拟人类行为
                if len(comments) >= self.max_comments:
                    break
                try:
                    async with page.expect_response(
                        lambda response: (
                            "bilibili.com/x/v2/reply/wbi/main" in response.url
                            and response.status == 200
                        ),
                        timeout=15 * 1000,
                    ) as response_info:
                        await page.goto(video_url, wait_until="domcontentloaded", timeout=20 * 1000)
                        await page.evaluate("window.scrollBy(0, 800)")

                    response = await response_info.value
                    try:
                        payload_text = await response.text()
                    except Exception as read_error:
                        logger.warning(f"Playwright 读取 B站评论响应失败 {video_url}: {read_error}")
                        continue
                    payload = json.loads(payload_text) if payload_text.strip() else {}
                    if not isinstance(payload, dict) or int(payload.get("code", -1)) != 0:
                        continue

                    data = payload.get("data", {})
                    replies = data.get("replies", []) if isinstance(data, dict) else []
                    if not isinstance(replies, list):
                        continue

                    for reply in replies:
                        if not isinstance(reply, dict):
                            continue

                        # 每条顶层 content 抓一条
                        top_content = reply.get("content", {})
                        if isinstance(top_content, dict):
                            top_text = str(top_content.get("message", "")).strip()
                            if top_text and self._append_comment(comments, top_text):
                                return comments[: self.max_comments]

                        # 每条顶层的 replies 抓一条（取第一条）
                        nested_replies = reply.get("replies", [])
                        if isinstance(nested_replies, list) and nested_replies:
                            first_nested = nested_replies[0]
                            if isinstance(first_nested, dict):
                                nested_content = first_nested.get("content", {})
                                if isinstance(nested_content, dict):
                                    nested_text = str(nested_content.get("message", "")).strip()
                                    if nested_text and self._append_comment(comments, nested_text):
                                        return comments[: self.max_comments]

                        if len(comments) >= self.max_comments:
                            return comments[: self.max_comments]
                except TimeoutError:
                    logger.warning(f"Playwright 获取 B站视频 {video_url} 评论超时")
                except Exception as video_e:
                    logger.warning(f"处理 B站单个视频 {video_url} 报错: {video_e}")
                    continue

            if not comments:
                logger.info(f"Playwright 最终为 B站标题 '{title}' 抓取到 0 条评论")
        except TimeoutError:
            logger.warning(f"Playwright 获取 B站评论超时: {title}")
        except Exception as e:
            logger.warning(f"crawl_bilibili_comments_optimized 获取评论阶段失败: {e}")
            return []
        finally:
            if page:
                browser_client = await self._get_browser_client()
                await browser_client.release_page(page)
        return comments

    # 监听response
    async def crawl_douyin_comments(self, title: str, url: str) -> List[str]:
        comments: List[str] = []
        page = None
        try:
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()
            await page.goto(url, wait_until="load", timeout=20*1000)
            for attempt in range(2):
                try:
                    async with page.expect_response(
                        lambda response: (
                            "douyin.com/aweme/v1/web/comment/list" in response.url
                            and response.status == 200
                        ),
                        timeout=20 * 1000,
                    ) as response_info:
                        if attempt == 0:

                            # 通过 locator 自动等待页面关键容器可见
                            await self._wait_locator_visible(
                                page,
                                'div[data-e2e="video-comment-more"], .video-detail-container, .main-container',
                                timeout=15000,
                            )

                        # 触发评论异步加载；第二次尝试会再滚一次，等真正的数据响应
                        await page.evaluate(f"window.scrollBy(0, {800 + attempt * 400})")

                    response = await response_info.value
                    try:
                        payload_text = await response.text()
                    except Exception as read_error:
                        if attempt > 0:
                            logger.warning(f"Playwright 读取抖音评论响应失败: {title}, attempt={attempt}, error={read_error}")
                        continue
                    if not payload_text.strip():
                        if attempt == 0:
                            continue
                        logger.info(f"Playwright 为标题 '{title}' 抓取到空的抖音评论响应")
                        continue

                    data = json.loads(payload_text)
                    for comment in data.get("comments", []):
                        text = comment.get("text", "")
                        if text and self._append_comment(comments, text):
                            return comments[: self.max_comments]

                    if comments:
                        break
                except TimeoutError:
                    if attempt == 0:
                        continue
                    logger.warning(f"Playwright 获取抖音评论超时: {title}")
                    continue
            
            if not comments:
                # 仅在失败且没有拿到评论时记录日志
                logger.info(f"Playwright 为标题 '{title}' 抓取到 0 条抖音评论")
            if page:
                try:
                    await page.screenshot(path=f"{self.screenshot_dir}/douyin_fatal_{int(time.time())}.png")
                except:
                    pass
        except Exception as e:
            logger.warning(f"crawl_douyin_comments 整体失败: {e}")
        finally:
            if page:
                browser_client = await self._get_browser_client()
                await browser_client.release_page(page)
        return comments[: self.max_comments]
    
    # 监听response
    async def crawl_toutiao_comments(self, title: str, url: str) -> List[str]:
        comments: List[str] = []
        page = None
        try:
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()

            try:
                async with page.expect_response(
                    lambda response: (
                        ("tab_comments" in response.url or "v2/comment/list" in response.url)
                        and response.status == 200
                    ),
                    timeout=15 * 1000,
                ) as response_info:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                    # Locator API 自动等待评论区容器
                    await self._wait_locator_visible(page, "div.comment-info", timeout=10000)

                response = await response_info.value
                payload_text = await response.text()
                data = json.loads(payload_text) if payload_text.strip() else {}
                items = data.get("data", [])
                if not isinstance(items, list):
                    items = data.get("data", {}).get("comments", [])

                for item in items:
                    comment_obj = item if "text" in item else item.get("comment", {})
                    text = comment_obj.get("text", "")
                    if text and self._append_comment(comments, text):
                        return comments[: self.max_comments]
            except TimeoutError:
                logger.warning(f"Playwright 获取头条评论超时: {title}")
                pass

            if not comments:
                logger.info(f"Playwright 为标题 '{title}' 抓获到 0 条头条评论")
        except Exception as e:
            try:
                if page:
                    await page.screenshot(path="screenshots/toutiao_fatal_error.png")
            except Exception:
                pass
            logger.warning(f"crawl_toutiao_comments 整体失败: {e}")
        finally:
            if page:
                browser_client = await self._get_browser_client()
                await browser_client.release_page(page)
        return comments[: self.max_comments]

    # 监听response
    # 贴吧的反爬机制较强，虽然流程正确，但是经常被验证码拦住，停用
    async def crawl_tieba_comments(self, title: str, url: str) -> List[str]:
        comments: List[str] = []
        page = None
        try:
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()

            try:
                async with page.expect_response(
                    lambda response: (
                        "tieba.baidu.com/hottopic/browse/getTopicRelateThread" in response.url
                        and response.status == 200
                    ),
                    timeout=15 * 1000,
                ) as response_info:
                    await page.goto(url, wait_until="domcontentloaded", timeout=12 * 1000)

                response = await response_info.value
                payload_text = await response.text()
                payload = json.loads(payload_text) if payload_text.strip() else {}
                if isinstance(payload, dict):
                    data = payload.get("data", {})
                    thread_list = data.get("thread_list", []) if isinstance(data, dict) else []
                    if isinstance(thread_list, list):
                        # 按浏览量从高到低排序后抓取
                        sorted_threads = sorted(
                            thread_list,
                            key=lambda x: int(x.get("view_num", 0) or 0) if isinstance(x, dict) else 0,
                            reverse=True,
                        )

                        for thread in sorted_threads:
                            if not isinstance(thread, dict):
                                continue

                            title_text = re.sub(r"\s+", " ", str(thread.get("title", "")).strip())
                            abstract_text = re.sub(r"\s+", " ", str(thread.get("abstract", "")).strip())

                            for text in (title_text, abstract_text):
                                if text and self._append_comment(comments, text):
                                    return comments[: self.max_comments]

                            if len(comments) >= self.max_comments:
                                return comments[: self.max_comments]
            except TimeoutError:
                logger.warning(f"Playwright 获取贴吧评论超时: {title}")
                pass

            if not comments:
                logger.info(f"Playwright 从贴吧热议/列表页直接抓取到 0 条（{title}+预览）内容")
        except Exception as e:
            try:
                if page:
                    await page.screenshot(path="screenshots/tieba_debug_fatal_error.png")
            except Exception:
                pass
            logger.warning(f"crawl_tieba_comments 整体失败: {e}")
        finally:
            if page:
                browser_client = await self._get_browser_client()
                await browser_client.release_page(page)
        return comments
    
    # 直接从页面 HTML 里解析 js-initialData，绕过复杂的 DOM 定位和反爬机制。适用于知乎等平台。
    async def crawl_zhihu_comments(self, title: str, url: str) -> List[str]:
        """从知乎页面的 js-initialData 中直接提取内容。"""

        def extract_initial_data(script_text: str) -> Optional[Dict]:
            # 直接读取 script 标签里的 JSON 内容，避免解析整页 HTML。
            payload = (script_text or "").strip()
            if not payload:
                return None

            try:
                data = json.loads(payload)
                return data if isinstance(data, dict) else None
            except Exception as e:
                logger.warning(f"解析知乎 js-initialData 失败: {e}")
                return None

        def append_html_field(comments_list: List[str], html_text: str) -> bool:
            # 先把 HTML 标签清掉，再走统一的去重和长度控制。
            if not html_text:
                return len(comments_list) >= self.max_comments

            text = BeautifulSoup(str(html_text), "html.parser").get_text(" ", strip=True)
            text = re.sub(r"\s+", " ", text).strip()
            if not text or text == "阅读全文":
                return len(comments_list) >= self.max_comments
            return self._append_comment(comments_list, text)

        def collect_comments(data: Dict) -> List[str]:
            # 优先抓热评，再补回答正文，尽量覆盖页面里最有信息量的内容。
            collected: List[str] = []
            entities = (data or {}).get("initialState", {}).get("entities", {})
            answers = entities.get("answers", {}) if isinstance(entities, dict) else {}
            if not isinstance(answers, dict):
                return collected

            for answer in answers.values():
                if not isinstance(answer, dict):
                    continue

                hot_comments = answer.get("hotComment", [])
                if isinstance(hot_comments, list):
                    for item in hot_comments:
                        if not isinstance(item, dict):
                            continue
                        if append_html_field(collected, item.get("content", "")):
                            return collected[: self.max_comments]

                if append_html_field(collected, answer.get("content", "")):
                    return collected[: self.max_comments]

            return collected[: self.max_comments]

        comments: List[str] = []
        page = None
        try:
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)

            # 先等目标 script 出现，再直接读取它的文本内容。
            script_locator = page.locator('script#js-initialData[type="text/json"]').first
            try:
                await script_locator.wait_for(state="attached", timeout=10000)
            except Exception:
                pass

            script_text = await script_locator.text_content()
            initial_data = extract_initial_data(script_text or "")
            if initial_data:
                comments = collect_comments(initial_data)

            if not comments:
                logger.info(f"Playwright 从知乎链接 '{url}' 的 js-initialData 中抓取到 0 条内容")
        except Exception as e:
            try:
                if page:
                    await page.screenshot(path="screenshots/zhihu_debug_fatal_error.png")
            except Exception:
                pass
            logger.warning(f"crawl_zhihu_comments 整体失败: {e}")
        finally:
            if page:
                browser_client = await self._get_browser_client()
                await browser_client.release_page(page)
        return comments[: self.max_comments]

    # 直接调用澎湃的评论 API，绕过页面复杂的反爬机制。适用于澎湃等平台。
    async def crawl_thepaper_comments(self, title: str, url: str) -> List[str]:
        """爬取澎湃新闻评论。"""
        comments: List[str] = []
        cont_id = None

        try:
            # 示例: https://www.thepaper.cn/newsDetail_forward_32870834
            match = re.search(r"newsDetail_forward_(\d+)", url or "")
            if match:
                cont_id = match.group(1)
                # 兜底：尝试从 URL 中提取连续数字
                fallback = re.search(r"(\d{6,})", url or "")
                if fallback:
                    cont_id = fallback.group(1)

            if not cont_id:
                logger.warning(f"无法从澎湃链接提取 contId: {url}")
                return []

            api_url = "https://api.thepaper.cn/comment/news/comment/talkList"
            proxies = {"http": self.proxy_url, "https": self.proxy_url} if self.proxy_url else None

            page_num = 1
            page_size = 20

            while len(comments) < self.max_comments:
                payload = {
                    "contId": str(cont_id),
                    "pageSize": page_size,
                    "commentSort": 1,
                    "contType": 1,
                    "pageNum": page_num,
                }

                # 使用 loop.run_in_executor 包装同步的 requests 调用，避免阻塞协程
                loop = asyncio.get_event_loop()
                def fetch_page():
                    return requests.post(
                        api_url,
                        json=payload,
                        headers=self.DEFAULT_HEADERS,
                        proxies=proxies,
                        timeout=15,
                    )
                
                resp = await loop.run_in_executor(None, fetch_page)
                resp.raise_for_status()

                data = resp.json() if resp.text else {}
                items = data.get("data", {}).get("list", [])
                has_next = bool(data.get("data", {}).get("hasNext", False))

                if not items:
                    break

                for item in items:
                    text = str(item.get("content", "")).strip()
                    if text and self._append_comment(comments, text):
                        return comments[: self.max_comments]

                if not has_next:
                    break

                page_num += 1
                await asyncio.sleep(random.uniform(2, 3))

            if not comments:
                logger.info(f"为澎湃标题 '{title}' 抓取到 0 条评论")
            return comments
        except Exception as e:
            logger.warning(f"crawl_thepaper_comments 整体失败: {e}")
            return []
        
    async def crawl_hupu_comments(self, title: str, url: str) -> List[str]:
        """爬取虎扑体育社区评论。"""
        comments: List[str] = []
        try:
            # 随机等待，降低请求频率
            await asyncio.sleep(random.uniform(1.5, 3.0))

            proxies = {"http": self.proxy_url, "https": self.proxy_url} if self.proxy_url else None
            
            loop = asyncio.get_event_loop()
            def fetch_hupu():
                return requests.get(
                    url,
                    headers=self.DEFAULT_HEADERS,
                    proxies=proxies,
                    timeout=15,
                )
            
            response = await loop.run_in_executor(None, fetch_hupu)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            detail_blocks = soup.find_all("div", class_="thread-content-detail")
            simple_detail_blocks = soup.find_all(
                "div",
                class_=lambda c: c and any(
                    cls.startswith("index_simple-detail-content__")
                    for cls in c.split()
                ),
            )

            for block in detail_blocks:
                p_nodes = block.find_all("p")
                for p in p_nodes:
                    text = p.get_text(" ", strip=True)
                    text = re.sub(r"\s+", " ", text).strip()
                    if not text:
                        continue
                    if self._append_comment(comments, text):
                        return comments[: self.max_comments]

                if len(comments) >= self.max_comments:
                    break

            if len(comments) < self.max_comments:
                for block in simple_detail_blocks:
                    p_nodes = block.find_all("p")
                    for p in p_nodes:
                        text = p.get_text(" ", strip=True)
                        text = re.sub(r"\s+", " ", text).strip()
                        if not text:
                            continue
                        if self._append_comment(comments, text):
                            return comments[: self.max_comments]

                    if len(comments) >= self.max_comments:
                        break

            if not comments:
                logger.info(f"为虎扑标题 '{title}' 抓取到 0 条评论")
            return comments
        except Exception as e:
            logger.warning(f"crawl_hupu_comments 整体失败: {e}")
            return []
        
    async def crawl_tencent_hot_comments(self, title: str, url: str) -> List[str]:
        """爬取腾讯新闻评论。"""
        comments: List[str] = []
        try:
            # 随机等待，降低请求频率
            await asyncio.sleep(random.uniform(2.0, 3.5))

            article_id = None
            # 示例: https://view.inews.qq.com/a/20260402A00UY000
            match = re.search(r"/(?:rain/)?a/([A-Za-z0-9]+)", url or "")
            if match:
                article_id = match.group(1)
            else:
                q_match = re.search(r"[?&]article_id=([A-Za-z0-9]+)", url or "")
                if q_match:
                    article_id = q_match.group(1)

            if not article_id:
                logger.warning(f"无法从腾讯新闻链接提取 article_id: {url}")
                return []

            api_url = "https://i.news.qq.com/getQQNewsComment"
            proxies = {"http": self.proxy_url, "https": self.proxy_url} if self.proxy_url else None
            req_num = max(5, min(self.max_comments, 50))

            params = {
                "apptype": "web",
                "article_id": article_id,
                "reqNum": str(req_num),
                "transparam": "",
            }

            loop = asyncio.get_event_loop()
            def fetch_tencent():
                return requests.get(
                    api_url,
                    params=params,
                    headers=self.DEFAULT_HEADERS,
                    proxies=proxies,
                    timeout=15,
                )
            
            resp = await loop.run_in_executor(None, fetch_tencent)
            resp.raise_for_status()
            data = resp.json() if resp.text else {}
            comments_obj = data.get("comments", {})

            def walk_nodes(node):
                if isinstance(node, dict):
                    text = str(node.get("reply_content") or node.get("content") or "").strip()
                    if text:
                        text = re.sub(r"\s+", " ", text)
                        if self._append_comment(comments, text):
                            return True
                    for v in node.values():
                        if walk_nodes(v):
                            return True
                elif isinstance(node, list):
                    for item in node:
                        if walk_nodes(item):
                            return True
                return len(comments) >= self.max_comments

            walk_nodes(comments_obj.get("new", []))
            if len(comments) < self.max_comments:
                walk_nodes(comments_obj.get("hot", []))

            if not comments:
                logger.info(f"为腾讯标题 '{title}' 抓取到 0 条评论")
            return comments[: self.max_comments]
        except Exception as e:
            logger.warning(f"crawl_tencent_hot_comments 整体失败: {e}")
            return []