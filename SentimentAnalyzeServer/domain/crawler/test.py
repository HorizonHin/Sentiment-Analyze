

import asyncio
import os
import sys
import json
import re
import random
from typing import List
import requests
import logging
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, "..", "..", "..")
if project_root not in sys.path:
    sys.path.append(project_root)
from async_browser_client import AsyncBrowserClient

logger = logging.getLogger(__name__)
# 将项目根目录添加到 python 路径，确保可以导入 SentimentAnalyzeServer

    # 反爬虫机制较强的平台，完全使用 Playwright。
async def crawl_weibo_comments(title: str,url: str) -> List[str]:
    comments: List[str] = []
    context = None
    page = None
    await asyncio.sleep(random.uniform(1.0, 3.0))  # 抓取前随机等待，模拟人类行为
    try:
        browser_client = await AsyncBrowserClient.get_instance(
            proxy_url=None,
            headers_config={},
            default_headers={},
            max_comments=10
        )
        
        # 定义 iPhone 动态 User-Agent 列表
        iphone_uas = [
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 15_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.3 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_8 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1"
        ]
        mobile_ua = random.choice(iphone_uas)
        
        context = await browser_client.new_isolated_context(user_agent=mobile_ua)
        await context.set_extra_http_headers(browser_client.get_headers("crawl_weibo_comments"))
        page = await context.new_page()
        search_url = url 

        # 创建一个 Future 对象用于控制任务状态
        crawl_future = asyncio.get_running_loop().create_future()

        # 使用事件监听拦截 API 响应
        async def handle_response(response):
            # 如果已经完成（无论是成功还是超时），不再处理
            if crawl_future.done():
                return
                
            if "api/container/getIndex" in response.url:
                try:
                    text = await response.text()
                    if not text.strip().startswith("{"):
                        return
                    data = json.loads(text)
                    cards = data.get("data", {}).get("cards", [])
                    for card in cards:
                        # 核心逻辑：直接从 card_type 9 的内容中提取微博正文作为“评论”
                        if card.get("card_type") == 9 and card.get("mblog"):
                            mblog = card.get("mblog", {})
                            raw_text = mblog.get("text", "")
                            # 清洗 HTML 标签
                            clean_text = re.sub(r"<[^>]+>", "", raw_text).strip()
                            if clean_text and clean_text not in comments:
                                comments.append(clean_text)
                    
                    # 如果拿到了足够的评论，标记 Future 为成功
                    if len(comments) >= 10:
                        if not crawl_future.done():
                            crawl_future.set_result(True)
                            
                except Exception as e:
                    logger.warning(f"解析响应出错: {e}")

        page.on("response", handle_response)
        
        await page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
         
        # 启动一个异步任务来循环滑动页面
        async def scroll_task():
            max_scroll_attempts = 3
            for i in range(max_scroll_attempts):
                if crawl_future.done():
                    break
                # 滚动到底部
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                # 等待数据加载
                await asyncio.sleep(random.uniform(2.0, 3.5))
                logger.info(f"第 {i+1} 次滑动加载，当前已获取内容数: {len(comments)}")
            
            # 如果循环结束还没满足数量，但也拿到了数据，也算某种程度的成功
            if not crawl_future.done():
                if len(comments) > 0:
                    crawl_future.set_result(True)
                else:
                    crawl_future.set_exception(Exception("滑动结束仍未获取到任何内容"))
        # 将滑动任务跑在后台
        scroller = asyncio.create_task(scroll_task())
        asyncio.wait_for(crawl_future, timeout=15.0)  # 整体超时控制，防止无限等待
        try:
            # 等待 Future 完成，设置总超时时间
            await asyncio.wait_for(crawl_future, timeout=45.0)
        except asyncio.TimeoutError:
            logger.warning(f"爬取任务超时: {title}")
            if not comments:
                return []
        finally:
            # 停止滑动任务
            if not scroller.done():
                scroller.cancel()

        if not comments:
            logger.info(f"Playwright 为标题 '{title}' 抓取到 0 条微博内容")
    except Exception as e:
        logger.warning(f"crawl_weibo_comments 整体失败: {e}")
        return []
    finally:
        if page:
            await page.close()
        if context:
            await context.close()
    return comments


async def test_weibo_comments():
    print("开始测试微博评论爬取...")
    # 初始化获取器，可以配置代理或调整最大评论数

    
    title = "上海疫情"
    url = "https://s.weibo.com/weibo?q=%E7%8E%8B%E6%BF%9B%20%E6%B5%AA%E5%A7%90%E7%9B%B4%E6%92%AD%E5%A4%AA%E7%A3%A8%E5%8F%BD%E4%BA%86&t=31&band_rank=9&Refer=top"
    try:
        # 注意：crawl_weibo_comments 是异步方法，且包含在 fetcher 类中
        # 根据 fetcher.py 定义，它的参数通常是 (title, url)
        comments = await crawl_weibo_comments(title, url)
        
        print(f"\n爬取完成，关键词: {title}")
        print(f"获取到评论数量: {len(comments)}")
        
        for i, comment in enumerate(comments, 1):
            print(f"{i}. {comment}")
            
    except Exception as e:
        print(f"测试过程中发生错误: {e}")

if __name__ == "__main__":
    asyncio.run(test_weibo_comments())
    
