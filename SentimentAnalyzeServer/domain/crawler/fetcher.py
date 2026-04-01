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
import time
import re
from typing import Dict, List, Tuple, Optional, Union
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

class DataFetcher:
    """数据获取器"""

    # 默认 API 地址
    DEFAULT_API_URL = "https://newsnow.busiyi.world/api/s"

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
    ):
        """
        初始化数据获取器

        Args:
            proxy_url: 代理服务器 URL（可选）
            api_url: API 基础 URL（可选，默认使用 DEFAULT_API_URL）
        """
        self.proxy_url = proxy_url
        self.api_url = api_url or self.DEFAULT_API_URL
        self._playwright = None
        self._browser = None

    def _get_browser(self):
        """
        获取或创建常驻的 Playwright 浏览器实例
        """
        if self._browser:
            return self._browser
            
        try:
            self._playwright = sync_playwright().start()
            browser_args = []
            if self.proxy_url:
                browser_args.append(f'--proxy-server={self.proxy_url}')
            self._browser = self._playwright.chromium.launch(headless=True, args=browser_args)
            return self._browser
        except Exception as e:
            logger.warning(f"启动 Playwright 浏览器失败: {e}")
            self.close()
            raise

    def close(self):
        """
        关闭浏览器和 Playwright 实例
        """
        try:
            if self._browser:
                self._browser.close()
                self._browser = None
            if self._playwright:
                self._playwright.stop()
                self._playwright = None
        except Exception as e:
            logger.warning(f"关闭浏览器实例失败: {e}")

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

    def crawl_comments_dispatch(self, source_id: str, title: str) -> List[str]:
        """
        根据 source_id 分发到不同的评论爬取方法
        """
        if "baidu" in source_id.lower():
            return self.crawl_baidu_comments(title)
        elif "weibo" in source_id.lower():
            return self.crawl_weibo_comments(title)
        elif "bilibili" in source_id.lower():
            return self.crawl_bilibili_comments(title)
        else:
            logger.warning(f"未知平台 {source_id}，无法抓取评论")
            return []
             
    def crawl_baidu_comments(self, title: str) -> List[str]:
        """
        爬取百度事件数据，返回评论区评论内容列表。使用 Playwright。
        """ 
        base_url = "https://www.baidu.com/s?wd="
        search_url = f"{base_url}{requests.utils.quote(title)}"
        comments = []

        context = None
        try:
            # 1. 使用常驻浏览器实例
            browser = self._get_browser()
            context = browser.new_context(user_agent=self.DEFAULT_HEADERS["User-Agent"])
            page = context.new_page()

            # 访问搜索结果页
            page.goto(search_url, wait_until="networkidle", timeout=30000)
            
            # 找到首条结果中的跳转链接
            link_selector = 'div[class*="title_1WDM0"] a'
            page.wait_for_selector(link_selector, timeout=10000)
            first_link = page.query_selector(link_selector)
            if not first_link:
                logger.warning(f"Playwright 无法定位到百度搜索首条链接")
                return []
                
            href = first_link.get_attribute('href')
            if not href:
                logger.warning("Playwright 抓取到空 href")
                return []
            
            # 在当前页跳转到详情页以获取评论
            page.goto(href, wait_until="networkidle", timeout=30000)
            
            # 等待并提取评论元素
            comment_selector = 'span[class*="type-text"]'
            page.wait_for_selector(comment_selector, timeout=10000)
            comment_elements = page.query_selector_all(comment_selector)
            comments = [el.inner_text().strip() for el in comment_elements if el.inner_text().strip()]
            logger.info(f"Playwright 为标题 '{title}' 抓取到 {len(comments)} 条百度评论")
        except Exception as e:
            logger.warning(f"crawl_baidu_comments 整体失败: {e}")
            return []
        finally:
            if context:
                context.close()
        return comments
        
    def crawl_weibo_comments(self, title: str) -> List[str]:
        """
        爬取微博事件数据，返回评论区评论内容列表。使用 Playwright 模拟 H5 接口。
        参考 MediaCrawler 实现。
        """
        comments = []
        context = None
        try:
            browser = self._get_browser()
            # 微博移动端需要特有的 User-Agent
            mobile_ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1"
            context = browser.new_context(user_agent=mobile_ua)
            page = context.new_page()

            # 1. 访问微博移动端主页初始化 Cookie
            page.goto("https://m.weibo.cn", wait_until="networkidle", timeout=30000)
            time.sleep(2)

            # 2. 调用搜索接口获取 mid
            # SearchType: 1-综合, 61-实时, 60-热门, 64-视频
            search_type = 60
            search_url = f"https://m.weibo.cn/api/container/getIndex?containerid=100103type={search_type}&q={requests.utils.quote(title)}&page_type=searchall"
            page.goto(search_url, wait_until="networkidle", timeout=30000)
            
            # 提取 JSON 内容
            json_text = page.evaluate("() => document.body.innerText")
            search_data = json.loads(json_text)

            cards = search_data.get("data", {}).get("cards", [])
            mid_list = []
            for card in cards:
                # 微博搜索 API 返回的 card_type 含义：
                # 9: 微博正文 (mblog)
                # 11: 卡片集合 (通常包含多个 card_group 内容)
                # 58: 搜索建议或相关词
                # 101: 话题/栏目信息
                if card.get("card_type") == 11:
                    card_group = card.get("card_group", [])
                    for item in card_group:
                        if item.get("mblog"):
                            mid_list.append(item.get("mblog", {}).get("id"))
                elif card.get("card_type") == 9:
                    if card.get("mblog"):
                        mid_list.append(card.get("mblog", {}).get("id"))
                
                # 限制抓取的微博正文数量，防止过量请求
                if len(mid_list) >= 5:
                    break

            if not mid_list:
                logger.warning(f"未找到任何微博 mid: {title}")
                return []

            # 3. 遍历 mid_list 获取每个微博的热评
            for mid in mid_list:
                try:
                    comments_url = f"https://m.weibo.cn/comments/hotflow?id={mid}&mid={mid}&max_id_type=0"
                    page.goto(comments_url, wait_until="networkidle", timeout=30000)
                    comments_json_text = page.evaluate("() => document.body.innerText")
                    # 判空处理，防止 API 返回非 JSON 内容
                    if not comments_json_text or not comments_json_text.strip().startswith('{'):
                        continue

                    comments_data = json.loads(comments_json_text)
                    raw_comments = comments_data.get("data", {}).get("data", [])
                    
                    for c in raw_comments:
                        text = c.get("text", "")
                        # 去除 HTML 标签 (微博评论中常含 <a> 或 <img>)
                        clean_text = re.sub(r'<[^>]+>', '', text).strip()
                        if clean_text:
                            comments.append(clean_text)
                    
                    # 适当休眠，模拟真实行为
                    time.sleep(1)
                except Exception as loop_e:
                    logger.warning(f"获取 mid={mid} 的评论时出错: {loop_e}")
                    continue
            
            # 去重并限制最终返回的总评论数量
            comments = list(dict.fromkeys(comments))[:30]
            logger.info(f"Playwright 为标题 '{title}' 抓取到 {len(comments)} 条微博评论")

        except Exception as e:
            logger.warning(f"crawl_weibo_comments 整体失败: {e}")
            return []
        finally:
            if context:
                context.close()
        return comments

    def crawl_bilibili_comments(self, title: str) -> List[str]:
        """
        爬取哔哩哔哩事件数据，返回评论区评论内容列表。
        """
        
        return []