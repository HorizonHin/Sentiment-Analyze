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
    
    # 支持抓取评论的平台集合。抖音等平台的评论反爬较强，暂不支持。
    SUPPORTED_COMMENT_PLATFORMS = {"baidu", "weibo", "bilibili","douyin", "toutiao", 
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
            max_comments: 最大抓取评论数量（默认 30）
        """
        self.proxy_url = proxy_url
        self.api_url = api_url or self.DEFAULT_API_URL
        self.max_comments = max_comments
        self._browser_client: Optional[AsyncBrowserClient] = None
        
        # 确保截图保存目录存在
        self.screenshot_dir = "screenshots"
        if not os.path.exists(self.screenshot_dir):
            os.makedirs(self.screenshot_dir)
            logger.info(f"创建截图目录: {self.screenshot_dir}")

        self.headers_config = self._load_headers_config()

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
        if self._browser_client is None:
            self._browser_client = await AsyncBrowserClient.get_instance(
                proxy_url=self.proxy_url,
                headers_config=self.headers_config,
                default_headers=self.DEFAULT_HEADERS,
                max_comments=self.max_comments,
                max_concurrent_pages=5,
            )
        return self._browser_client

    async def close(self):
        """
        关闭异步浏览器客户端 (异步)
        """
        if self._browser_client:
            await self._browser_client.close()
            self._browser_client = None

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
        if "baidu" in source_id.lower():
            return await self.crawl_baidu_comments_opyimized(title, url)
        elif "weibo" in source_id.lower():
            return await self.crawl_weibo_comments(title)
        elif "bilibili" in source_id.lower():
            return await self.crawl_bilibili_comments_optimized(title, url)  
        elif "douyin" in source_id.lower():
            return await self.crawl_douyin_comments(title, url)                  
        elif "toutiao" in source_id.lower():
            return await self.crawl_toutiao_comments(title, url)
        elif "zhihu" in source_id.lower():
            return await self.crawl_zhihu_comments(title, url)
        elif "tieba" in source_id.lower():
            return await self.crawl_tieba_comments(title, url)
        elif "thepaper" in source_id.lower() or "pengpai" in source_id.lower():
            return self.crawl_thepaper_comments(title, url)
        elif "hupu" in source_id.lower():
            return self.crawl_hupu_comments(title, url)
        elif "tencent-hot" in source_id.lower():
            return self.crawl_tencent_hot_comments(title, url)
        else:
            logger.warning(f"未知平台 {source_id}，无法抓取评论")
            return []
           
    async def crawl_baidu_comments_opyimized(self, title: str, url: str) -> List[str]:
        search_url = url
        comments: List[str] = []
        page = None

        try:
            await asyncio.sleep(random.uniform(2.5, 3.5))

            proxies = {"http": self.proxy_url, "https": self.proxy_url} if self.proxy_url else None
            search_response = requests.get(search_url, headers=self.DEFAULT_HEADERS, proxies=proxies, timeout=15)
            search_response.raise_for_status()
            soup = BeautifulSoup(search_response.text, "html.parser")

            first_link = None
            link_el = soup.select_one('div[class*="title_1WDM0"] a')
            if link_el and link_el.get("href"):
                first_link = link_el
            else:
                fallback_el = soup.select_one('div[class*="result"] h3 a') or soup.find("a", {"class": "c-showurl"})
                if fallback_el and fallback_el.get("href"):
                    first_link = fallback_el

            if not first_link:
                logger.warning(f"BeautifulSoup 无法定位到百度搜索首条链接: {title}")
                return []

            href = first_link.get("href")
            if not href:
                logger.warning("BeautifulSoup 抓取到空 href")
                return []
            if href.startswith("/s?"):
                href = f"https://www.baidu.com{href}"

            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()
            await page.goto(href, wait_until="networkidle", timeout=30000)

            comment_selector = 'span[class*="type-text"]'
            await page.wait_for_selector(comment_selector, timeout=10000)
            comment_elements = await page.query_selector_all(comment_selector)
            for el in comment_elements:
                text = (await el.inner_text()).strip()
                if text:
                    comments.append(text)
                if len(comments) >= self.max_comments:
                    break

            if not comments:
                logger.info(f"Playwright 为标题 '{title}' 抓取到 0 条百度评论")
        except Exception as e:
            logger.warning(f"crawl_baidu_comments_opyimized 整体失败: {e}")
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
        try:
            browser_client = await self._get_browser_client()
            mobile_ua = (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 "
                "Mobile/15E148 Safari/604.1"
            )
            context = await browser_client.new_isolated_context(user_agent=mobile_ua)
            context.set_extra_http_headers(browser_client.get_headers("crawl_weibo_comments"))
            page = await context.new_page()

            await page.goto("https://m.weibo.cn", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            search_type = 60
            search_url = (
                "https://m.weibo.cn/api/container/getIndex?containerid=100103type="
                f"{search_type}&q={requests.utils.quote(title)}&page_type=searchall"
            )
            await page.goto(search_url, wait_until="networkidle", timeout=30000)

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
                if len(mid_list) >= 30:
                    break

            if not mid_list:
                logger.warning(f"未找到任何微博 mid: {title}")
                return []

            for mid in mid_list:
                try:
                    comments_url = f"https://m.weibo.cn/comments/hotflow?id={mid}&mid={mid}&max_id_type=0"
                    await page.goto(comments_url, wait_until="networkidle", timeout=30000)
                    comments_json_text = await page.evaluate("() => document.body.innerText")
                    if not comments_json_text or not comments_json_text.strip().startswith("{"):
                        continue

                    comments_data = json.loads(comments_json_text)
                    raw_comments = comments_data.get("data", {}).get("data", [])
                    for c in raw_comments:
                        text = c.get("text", "")
                        clean_text = re.sub(r"<[^>]+>", "", text).strip()
                        if clean_text and clean_text not in comments:
                            comments.append(clean_text)
                        if len(comments) >= self.max_comments:
                            break
                    if len(comments) >= self.max_comments:
                        break
                    await asyncio.sleep(1)
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
                if len(comments) >= self.max_comments:
                    break
                try:
                    await page.goto(video_url, wait_until="networkidle", timeout=20000)
                    await page.evaluate("window.scrollBy(0, 800)")
                    await asyncio.sleep(2.5)

                    comments_script = """
                    () => {
                        const results = [];
                        const commentsApp = document.querySelector("#commentapp > bili-comments");
                        if (!commentsApp || !commentsApp.shadowRoot) return results;
                        const threads = commentsApp.shadowRoot.querySelectorAll("#feed > bili-comment-thread-renderer");
                        for (const thread of threads) {
                            if (!thread.shadowRoot) continue;
                            const commentNode = thread.shadowRoot.querySelector("#comment");
                            if (commentNode && commentNode.shadowRoot) {
                                const richText = commentNode.shadowRoot.querySelector("#content > bili-rich-text");
                                if (richText && richText.shadowRoot) {
                                    const textSpan = richText.shadowRoot.querySelector("#contents > span");
                                    if (textSpan && textSpan.innerText.trim()) {
                                        results.push(textSpan.innerText.trim());
                                    }
                                }
                            }
                            const repliesRenderer = thread.shadowRoot.querySelector("#replies > bili-comment-replies-renderer");
                            if (repliesRenderer && repliesRenderer.shadowRoot) {
                                const firstReply = repliesRenderer.shadowRoot.querySelector("#expander-contents > bili-comment-reply-renderer");
                                if (firstReply && firstReply.shadowRoot) {
                                    const replyRichText = firstReply.shadowRoot.querySelector("#main > bili-rich-text");
                                    if (replyRichText && replyRichText.shadowRoot) {
                                        const replySpan = replyRichText.shadowRoot.querySelector("#contents > span");
                                        if (replySpan && replySpan.innerText.trim()) {
                                            results.push(replySpan.innerText.trim());
                                        }
                                    }
                                }
                            }
                            if (results.length >= 10) break;
                        }
                        return results;
                    }
                    """

                    extracted_texts = await page.evaluate(comments_script)
                    for text in extracted_texts:
                        if text and text not in comments:
                            comments.append(text)
                        if len(comments) >= self.max_comments:
                            break
                except Exception as video_e:
                    logger.warning(f"处理 B站单个视频 {video_url} 报错: {video_e}")
                    continue

            if not comments:
                logger.info(f"Playwright 最终为 B站标题 '{title}' 抓取到 0 条评论")
        except Exception as e:
            logger.warning(f"crawl_bilibili_comments_optimized 获取评论阶段失败: {e}")
            return []
        finally:
            if page:
                browser_client = await self._get_browser_client()
                await browser_client.release_page(page)
        return comments

    async def crawl_bilibili_comments(self, title: str, url: str = None) -> List[str]:
        comments: List[str] = []
        page = None
        try:
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()
            search_url = url or f"https://search.bilibili.com/all?keyword={requests.utils.quote(title)}"
            await page.goto(search_url, wait_until="networkidle", timeout=30000)

            video_link_selector = '.bili-video-card__info--right a'
            try:
                await page.wait_for_selector(video_link_selector, timeout=10000)
            except Exception:
                logger.warning(f"搜索结果未及时加载: {title}")
                return []

            video_links = await page.query_selector_all(video_link_selector)
            video_urls = []
            for link in video_links:
                href = await link.get_attribute("href")
                if href:
                    video_url = "https:" + href if href.startswith("//") else href
                    video_urls.append(video_url)

            if not video_urls:
                logger.warning(f"无法找到 Bilibili 搜索结果: {title}")
                return []

            for video_url in video_urls:
                if len(comments) >= self.max_comments:
                    break
                try:
                    await page.goto(video_url, wait_until="networkidle", timeout=20000)
                    await page.evaluate("window.scrollBy(0, 800)")
                    await asyncio.sleep(2.5)

                    comments_script = """
                    () => {
                        const results = [];
                        const commentsApp = document.querySelector("#commentapp > bili-comments");
                        if (!commentsApp || !commentsApp.shadowRoot) return results;
                        const threads = commentsApp.shadowRoot.querySelectorAll("#feed > bili-comment-thread-renderer");
                        for (const thread of threads) {
                            if (!thread.shadowRoot) continue;
                            const commentNode = thread.shadowRoot.querySelector("#comment");
                            if (commentNode && commentNode.shadowRoot) {
                                const richText = commentNode.shadowRoot.querySelector("#content > bili-rich-text");
                                if (richText && richText.shadowRoot) {
                                    const textSpan = richText.shadowRoot.querySelector("#contents > span");
                                    if (textSpan && textSpan.innerText.trim()) {
                                        results.push(textSpan.innerText.trim());
                                    }
                                }
                            }
                            const repliesRenderer = thread.shadowRoot.querySelector("#replies > bili-comment-replies-renderer");
                            if (repliesRenderer && repliesRenderer.shadowRoot) {
                                const firstReply = repliesRenderer.shadowRoot.querySelector("#expander-contents > bili-comment-reply-renderer");
                                if (firstReply && firstReply.shadowRoot) {
                                    const replyRichText = firstReply.shadowRoot.querySelector("#main > bili-rich-text");
                                    if (replyRichText && replyRichText.shadowRoot) {
                                        const replySpan = replyRichText.shadowRoot.querySelector("#contents > span");
                                        if (replySpan && replySpan.innerText.trim()) {
                                            results.push(replySpan.innerText.trim());
                                        }
                                    }
                                }
                            }
                            if (results.length >= 10) break;
                        }
                        return results;
                    }
                    """

                    extracted_texts = await page.evaluate(comments_script)
                    for text in extracted_texts:
                        if text and text not in comments:
                            comments.append(text)
                        if len(comments) >= self.max_comments:
                            break
                except Exception as video_e:
                    logger.warning(f"处理 B站单个视频 {video_url} 报错: {video_e}")
                    continue

            logger.info(f"Playwright 最终为 B站标题 '{title}' 抓取到 {len(comments)} 条评论")
        except Exception as e:
            logger.warning(f"crawl_bilibili_comments 整体失败: {e}")
            return []
        finally:
            if page:
                browser_client = await self._get_browser_client()
                await browser_client.release_page(page)
        return comments

    async def crawl_douyin_comments(self, title: str, url: str) -> List[str]:
        comments: List[str] = []
        page = None
        try:
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()

            async def handle_response(response):
                try:
                    if "douyin.com/aweme/v1/web/comment/list" in response.url and response.status == 200:
                        data = await response.json()
                        for comment in data.get("comments", []):
                            text = comment.get("text", "")
                            if text and text not in comments:
                                comments.append(text)
                except Exception:
                    pass

            page.on("response", handle_response)
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(5)
            if not comments:
                await page.screenshot(path="screenshots/douyin_no_comments.png")
                logger.info(f"Playwright 为标题 '{title}' 抓取到 0 条抖音评论")
        except Exception as e:
            try:
                if page:
                    await page.screenshot(path="screenshots/douyin_fatal_error.png")
            except Exception:
                pass
            logger.warning(f"crawl_douyin_comments 整体失败: {e}")
        finally:
            if page:
                browser_client = await self._get_browser_client()
                await browser_client.release_page(page)
        return comments[: self.max_comments]
    
    async def crawl_toutiao_comments(self, title: str, url: str) -> List[str]:
        comments: List[str] = []
        page = None
        try:
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()

            async def handle_response(response):
                try:
                    if ("tab_comments" in response.url or "v2/comment/list" in response.url) and response.status == 200:
                        data = await response.json()
                        items = data.get("data", [])
                        if not isinstance(items, list):
                            items = data.get("data", {}).get("comments", [])

                        for item in items:
                            comment_obj = item if "text" in item else item.get("comment", {})
                            text = comment_obj.get("text", "")
                            if text and text not in comments:
                                comments.append(text)
                except Exception:
                    pass

            page.on("response", handle_response)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)
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

    # 贴吧的反爬机制较强，虽然流程正确，但是经常被验证码拦住，停用
    async def crawl_tieba_comments(self, title: str, url: str) -> List[str]:
        comments: List[str] = []
        page = None
        try:
            await asyncio.sleep(random.uniform(2.0, 3.0))
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=8*1000)
            await asyncio.sleep(2)

            thread_items = await page.query_selector_all("li.thread-item")
            for item in thread_items:
                title_el = await item.query_selector(".track-thread-title")
                if title_el:
                    title_text = (await title_el.inner_text()).strip()
                    if title_text and title_text not in comments:
                        comments.append(title_text)
                if len(comments) >= self.max_comments:
                    break

                content_el = await item.query_selector("p.content")
                if content_el:
                    content_text = (await content_el.inner_text()).strip()
                    if content_text and content_text not in comments:
                        comments.append(content_text)
                if len(comments) >= self.max_comments:
                    break

            if not comments:
                logger.info(f"Playwright 从贴吧热议/列表页直接抓取到 0 条（标题+预览）内容")
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
 
    async def crawl_zhihu_comments(self, title: str, url: str) -> List[str]:
        comments: List[str] = []
        page = None
        try:
            browser_client = await self._get_browser_client()
            page = await browser_client.acquire_page()
            await page.goto(url, wait_until="networkidle", timeout=20000)
            await page.screenshot(path="screenshots/zhihu_debug_page_loaded.png")

            content_blocks = await page.query_selector_all(".RichContent-inner")
            for block in content_blocks:
                content_span = await block.query_selector('xpath=.//*[@id="content"]/span[1]')
                if not content_span:
                    continue

                text = (await content_span.inner_text()).strip()
                if not text:
                    continue

                text = re.sub(r"\s+", " ", text).strip()
                if text == "阅读全文":
                    continue

                if text not in comments:
                    comments.append(text)
                    if len(comments) >= self.max_comments:
                        break

            if not comments:
                logger.info(f"Playwright 从知乎链接 '{url}' 直接抓取到 0 条内容")
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
        return comments
        
    def crawl_thepaper_comments(self, title: str, url: str) -> List[str]:
        """爬取澎湃新闻评论。"""
        comments: List[str] = []
        cont_id = None

        try:
            # 示例: https://www.thepaper.cn/newsDetail_forward_32870834
            match = re.search(r"newsDetail_forward_(\d+)", url or "")
            if match:
                cont_id = match.group(1)
            else:
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
                    "commentSort": 2,
                    "contType": 2,
                    "pageNum": page_num,
                }

                resp = requests.post(
                    api_url,
                    json=payload,
                    headers=self.DEFAULT_HEADERS,
                    proxies=proxies,
                    timeout=15,
                )
                resp.raise_for_status()

                data = resp.json() if resp.text else {}
                items = data.get("data", {}).get("list", [])
                has_next = bool(data.get("data", {}).get("hasNext", False))

                if not items:
                    break

                for item in items:
                    text = str(item.get("content", "")).strip()
                    if text and text not in comments:
                        comments.append(text)
                        if len(comments) >= self.max_comments:
                            break

                if not has_next:
                    break

                page_num += 1
                time.sleep(random.uniform(2, 3))

            if not comments:
                logger.info(f"为澎湃标题 '{title}' 抓取到 0 条评论")
            return comments
        except Exception as e:
            logger.warning(f"crawl_thepaper_comments 整体失败: {e}")
            return []
        
    def crawl_hupu_comments(self, title: str, url: str) -> List[str]:
        """爬取虎扑体育社区评论。"""
        comments: List[str] = []
        try:
            # 随机等待，降低请求频率
            time.sleep(random.uniform(1.5, 3.0))

            proxies = {"http": self.proxy_url, "https": self.proxy_url} if self.proxy_url else None
            response = requests.get(
                url,
                headers=self.DEFAULT_HEADERS,
                proxies=proxies,
                timeout=15,
            )
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
                    if text not in comments:
                        comments.append(text)
                        if len(comments) >= self.max_comments:
                            break

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
                        if text not in comments:
                            comments.append(text)
                            if len(comments) >= self.max_comments:
                                break

                    if len(comments) >= self.max_comments:
                        break

            if not comments:
                logger.info(f"为虎扑标题 '{title}' 抓取到 0 条评论")
            return comments
        except Exception as e:
            logger.warning(f"crawl_hupu_comments 整体失败: {e}")
            return []
        
    def crawl_tencent_hot_comments(self, title: str, url: str) -> List[str]:
        """爬取腾讯新闻评论。"""
        comments: List[str] = []
        try:
            # 随机等待，降低请求频率
            time.sleep(random.uniform(1.0, 2.5))

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

            resp = requests.get(
                api_url,
                params=params,
                headers=self.DEFAULT_HEADERS,
                proxies=proxies,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json() if resp.text else {}
            comments_obj = data.get("comments", {})

            def walk_nodes(node):
                if isinstance(node, dict):
                    text = str(node.get("reply_content") or node.get("content") or "").strip()
                    if text:
                        text = re.sub(r"\s+", " ", text)
                        if text not in comments:
                            comments.append(text)
                    for v in node.values():
                        walk_nodes(v)
                elif isinstance(node, list):
                    for item in node:
                        walk_nodes(item)

            walk_nodes(comments_obj.get("new", []))
            if len(comments) < self.max_comments:
                walk_nodes(comments_obj.get("hot", []))

            if not comments:
                logger.info(f"为腾讯标题 '{title}' 抓取到 0 条评论")
            return comments[: self.max_comments]
        except Exception as e:
            logger.warning(f"crawl_tencent_hot_comments 整体失败: {e}")
            return []