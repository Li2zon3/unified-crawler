#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一爬虫工具 (Unified Crawler)
==============================
整合三大数据源的爬取与下载：
  1. sse-search   : 上交所全站搜索爬虫（基于 ES 搜索接口，按关键词回溯）
  2. sse-inquiry  : 上交所问询函专栏爬虫（基于专栏 API，含防盗链下载）
  3. cninfo       : 巨潮资讯网公告下载器（从 Excel 读取链接批量下载）
  4. cninfo-search: 巨潮资讯网关键词检索（建索引 + 按索引下载）

安装依赖:
    pip install curl_cffi playwright pandas openpyxl tqdm
    playwright install chromium

用法:
    # === 上交所搜索 (sse-search) ===
    python unified_crawler.py sse-search --keyword <关键词>                   # 全自动：爬取 -> 合并 -> 下载
    python unified_crawler.py sse-search --keyword <关键词> --step crawl      # 仅爬取
    python unified_crawler.py sse-search --keyword <关键词> --step merge      # 仅合并
    python unified_crawler.py sse-search --keyword <关键词> --step download   # 仅下载
    python unified_crawler.py sse-search --keyword 年报 --output ./data       # 自定义关键词和目录

    # === 上交所问询函专栏 (sse-inquiry) ===
    python unified_crawler.py sse-inquiry                  # 爬取全部
    python unified_crawler.py sse-inquiry --step test      # 测试连通性
    python unified_crawler.py sse-inquiry --step download  # 下载文件
    python unified_crawler.py sse-inquiry --step verify    # 核对并补录
    python unified_crawler.py sse-inquiry --step dedup     # 文件去重
    python unified_crawler.py sse-inquiry --json xxx.json  # 指定 JSON 下载

    # === 巨潮资讯网 (cninfo) ===
    python unified_crawler.py cninfo sample.xlsx                       # 下载全部
    python unified_crawler.py cninfo sample.xlsx -o ./公告文件         # 指定目录
    python unified_crawler.py cninfo sample.xlsx --start 0 --end 10   # 指定范围

    # === 巨潮资讯网 - 从 Excel 链接列下载 (cninfo-excel) ===
    python unified_crawler.py cninfo-excel links.xlsx -o ./output --col 0

    # === 巨潮资讯网 - 关键词检索并下载 (cninfo-search) ===
    python unified_crawler.py cninfo-search <关键词> --step index
    python unified_crawler.py cninfo-search <关键词> --step download
    python unified_crawler.py cninfo-search <关键词> --step all --start-date 2026-01-01 --end-date 2026-02-06
"""

import hashlib
import json
import os
import sys
import re
import time
import glob
import random
import asyncio
import csv
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- 第三方依赖（延迟导入以显示友好错误） ----------
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import requests as std_requests
except ImportError:
    std_requests = None

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


def _require(lib_obj, name: str, pip_name: str = None):
    """检查依赖是否已安装"""
    if lib_obj is None:
        pip_name = pip_name or name
        print(f"❌ 缺少依赖: {name}，请运行: pip install {pip_name}")
        sys.exit(1)


# ╔══════════════════════════════════════════════════════════════════╗
# ║                        全 局 配 置                              ║
# ╚══════════════════════════════════════════════════════════════════╝

# --- 上交所搜索 (sse-search) ---
DEFAULT_OUTPUT_ROOT = 'output'
SSE_SEARCH_OUTPUT_DIR = os.path.join(DEFAULT_OUTPUT_ROOT, 'sse_search')
SSE_SEARCH_MERGED_FILE = 'all_merged_results.json'
SSE_SEARCH_MAX_EMPTY_YEARS = 3          # 连续 N 年无数据则停止回溯

# --- 上交所问询函专栏 (sse-inquiry) ---
SSE_INQUIRY_OUTPUT_DIR = os.path.join(DEFAULT_OUTPUT_ROOT, 'sse_inquiry')
SSE_INQUIRY_PAGE_SIZE = 25

# --- 巨潮资讯网 (cninfo) ---
CNINFO_OUTPUT_DIR = os.path.join(DEFAULT_OUTPUT_ROOT, 'cninfo')

# --- 通用 ---
MAX_DOWNLOAD_WORKERS = 3                # 下载并发数（过高容易被封）


# ╔══════════════════════════════════════════════════════════════════╗
# ║                      公 共 工 具 函 数                             ║
# ╚══════════════════════════════════════════════════════════════════╝

def parse_jsonp(text: str) -> dict:
    """解析 JSONP 响应，提取 JSON 数据"""
    match = re.search(r'jsonpCallback\d+\((.*)\)', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            pass
    return None


def save_to_csv(data: list, csv_path: str, fieldnames: list):
    """将列表数据保存为 CSV"""
    try:
        with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
        return True
    except Exception as e:
        print(f"    ⚠️ CSV 保存失败: {e}")
        return False


def calculate_md5(filepath: str) -> str:
    """计算文件 MD5 哈希（分块读取，防止大文件撑爆内存）"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def safe_filename(text: str, max_len: int = 50) -> str:
    """将文本转为安全文件名"""
    return re.sub(r'[\\/*?:"<>|\r\n]', '', str(text))[:max_len]


# ╔══════════════════════════════════════════════════════════════════╗
# ║          模块一：上交所全站搜索爬虫 (SSE Search)                 ║
# ╚══════════════════════════════════════════════════════════════════╝

class SSESearchCrawler:
    """
    通过上交所 ES 搜索接口，按关键词 + 时间段爬取数据。
    支持自动按年回溯、递归拆分大数据区间。
    来源: sse_spider.py
    """

    def __init__(self, output_dir: str, keyword: str):
        _require(cffi_requests, 'curl_cffi')
        if not keyword:
            raise ValueError("sse-search 关键词不能为空，请使用 --keyword 传入。")
        self.output_dir = output_dir
        self.keyword = keyword
        self.base_url = "https://query.sse.com.cn/search/getESSearchDoc.do"
        self.site_base = "https://www.sse.com.cn"
        self.session = cffi_requests.Session(impersonate="chrome124")
        self.headers = {
            'Referer': 'https://www.sse.com.cn/home/search/',
            'Origin': 'https://www.sse.com.cn',
            'Accept': '*/*',
        }
        self.session.headers.update(self.headers)
        os.makedirs(self.output_dir, exist_ok=True)
        self._init_session()

    def _init_session(self):
        print(">>> [SSE搜索] 初始化会话...")
        try:
            self.session.get("https://www.sse.com.cn/home/search/", timeout=15)
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ [SSE搜索] 初始化警告: {e}")

    # ---------- 时间格式化 ----------
    @staticmethod
    def format_time(date_str: str, is_end: bool = False) -> str:
        if not date_str:
            return ''
        suffix = " 23:59:59" if is_end else " 00:00:00"
        return f"{date_str}{suffix}" if ' ' not in date_str else date_str

    # ---------- 查询总数 ----------
    def check_total_count(self, start_date: str, end_date: str) -> int:
        """查询某时间段的数据总量"""
        params = {
            'jsonCallBack': f'jsonpCallback{random.randint(100000, 999999)}',
            'searchword': '', 'page': 0, 'limit': 1, 'spaceId': 3,
            'searchMode': 'precise', 'keyword': self.keyword, 'siteName': 'sse',
            'keywordPosition': 'title,paper_content',
            'publishTimeStart': self.format_time(start_date),
            'publishTimeEnd': self.format_time(end_date, True),
            'channelId': '10001', '_': int(time.time() * 1000)
        }
        try:
            resp = self.session.get(self.base_url, params=params, timeout=15)
            data = parse_jsonp(resp.text)
            if data and data.get('code') == '0':
                return int(data.get('data', {}).get('totalSize', 0))
        except Exception as e:
            print(f"    [!] 查询异常: {e}")
        return 0

    # ---------- 分页爬取 ----------
    def search_all(self, start_date: str, end_date: str) -> list:
        """爬取指定时间段内的全部搜索结果"""
        all_results = []
        page = 0  # 从 0 开始，否则会漏掉第一页

        while True:
            params = {
                'jsonCallBack': f'jsonpCallback{random.randint(100000, 999999)}',
                'searchword': '', 'page': page, 'limit': 20, 'spaceId': 3,
                'orderByDirection': 'DESC', 'orderByKey': 'score',
                'searchMode': 'precise', 'keyword': self.keyword, 'siteName': 'sse',
                'keywordPosition': 'title,paper_content',
                'publishTimeStart': self.format_time(start_date),
                'publishTimeEnd': self.format_time(end_date, True),
                'channelId': '10001', '_': int(time.time() * 1000)
            }
            try:
                resp = self.session.get(self.base_url, params=params, timeout=20)
                data = parse_jsonp(resp.text)
                if not data or data.get('code') != '0':
                    break

                k_list = data['data'].get('knowledgeList', [])
                if not k_list:
                    break

                for item in k_list:
                    res = self._parse_item(item)
                    if res:
                        all_results.append(res)

                total_page = data['data'].get('totalPage', 0)
                print(f"    >>> 抓取中: {start_date}~{end_date} | 页码: {page + 1}/{total_page} | 已抓: {len(all_results)}")

                if page >= total_page - 1:
                    break

                page += 1
                time.sleep(random.uniform(0.5, 1.0))
            except Exception as e:
                print(f"    异常重试: {e}")
                time.sleep(2)
        return all_results

    # ---------- 解析单条记录 ----------
    def _parse_item(self, item: dict) -> dict:
        if not item:
            return None
        extend = {ext['name']: ext.get('value')
                  for ext in (item.get('extend') or []) if ext.get('name')}

        title = re.sub(r'</?em>', '', str(item.get('title') or ''))
        curl = extend.get('CURL', '')
        file_url = (f"{self.site_base}{curl}" if curl and not curl.startswith('http')
                    else curl or item.get('url', ''))
        stock_code = extend.get('ZQDM', 'unknown')
        file_type = extend.get('FILETYPE', 'pdf')
        create_time = item.get('createTime', '')
        date_str = str(create_time)[:10].replace('-', '')
        s_title = safe_filename(title)

        return {
            'title': title, 'url': file_url, 'stock_code': stock_code,
            'stock_name': extend.get('GSJC', ''), 'create_time': create_time,
            'file_type': file_type,
            'local_filename': f"{stock_code}_{date_str}_{s_title}.{file_type}"
        }

    # ---------- 递归拆分时间段 ----------
    def run_recursive(self, start_date: str, end_date: str) -> int:
        """递归拆分时间段，直到数量适合爬取"""
        total = self.check_total_count(start_date, end_date)
        if total == 0:
            return 0

        # 数量过多，二分拆分
        if total > 4800:
            dt_start = datetime.strptime(start_date, "%Y-%m-%d")
            dt_end = datetime.strptime(end_date, "%Y-%m-%d")
            mid = (dt_start + (dt_end - dt_start) / 2).strftime("%Y-%m-%d")
            if mid == end_date:
                mid = start_date
            next_day = (datetime.strptime(mid, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            return self.run_recursive(start_date, mid) + self.run_recursive(next_day, end_date)

        # 数量合适，直接爬取
        print(f"  + 区间 [{start_date} ~ {end_date}] 发现 {total} 条数据，开始下载列表...")
        results = self.search_all(start_date, end_date)

        if results:
            base_name = f"{self.keyword}_{start_date.replace('-', '')}_{end_date.replace('-', '')}"
            fieldnames = ['stock_code', 'stock_name', 'title', 'url',
                          'create_time', 'file_type', 'local_filename']

            # 保存 JSON
            json_fpath = os.path.join(self.output_dir, f"{base_name}.json")
            with open(json_fpath, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

            # 保存 CSV
            csv_fpath = os.path.join(self.output_dir, f"{base_name}.csv")
            if save_to_csv(results, csv_fpath, fieldnames):
                print(f"    ✅ 已双重保存: {base_name}.json & {base_name}.csv")

        return len(results)


# ---------- SSE Search 合并 ----------
def sse_search_merge(data_dir: str, keyword: str, merged_filename: str) -> str:
    """合并所有 SSE 搜索结果 JSON 文件（去重）"""
    print(f"\n>>> [阶段2] 开始合并所有 JSON 文件...")
    patterns = [os.path.join(data_dir, f'{keyword}*.json')]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))

    target_files = sorted([f for f in files if 'merged' not in f])
    if not target_files:
        print("    未找到任何数据文件。")
        return None

    all_data = []
    seen = set()
    for jf in target_files:
        try:
            with open(jf, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    if item.get('url') and item['url'] not in seen:
                        seen.add(item['url'])
                        all_data.append(item)
        except Exception:
            pass

    # 保存 JSON
    out_path = os.path.join(data_dir, merged_filename)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    # 保存 CSV
    csv_path = out_path.replace('.json', '.csv')
    fieldnames = ['stock_code', 'stock_name', 'title', 'url',
                  'create_time', 'file_type', 'local_filename']
    if save_to_csv(all_data, csv_path, fieldnames):
        print(f"    ✅ CSV 已生成: {csv_path}")

    print(f"    ✅ 合并完成！总计有效记录: {len(all_data)} 条")
    print(f"    汇总文件: {out_path}")
    return out_path


# ---------- SSE Search Playwright 下载 ----------
async def _solve_waf(context, url):
    """解瑞数反爬盾"""
    page = await context.new_page()
    try:
        await page.goto(url, timeout=15000, wait_until='domcontentloaded')
        await asyncio.sleep(3)
    except Exception:
        pass
    finally:
        await page.close()


async def _playwright_download_file(context, url, path):
    """使用 Playwright 下载单个文件"""
    try:
        resp = await context.request.get(url, timeout=20000)
        body = await resp.body()

        is_waf = False
        if b'var arg1=' in body or b'var _0x' in body:
            is_waf = True
        elif body[:4] != b'%PDF' and len(body) < 6000 and b'<html' in body:
            is_waf = True

        if is_waf:
            print(" -> 🛡️ 触发反爬，解盾中...", end="")
            await _solve_waf(context, url)
            resp = await context.request.get(url, timeout=20000)
            body = await resp.body()
            if body[:4] == b'%PDF':
                print(" -> ✅ 成功", end=" ")
            else:
                return False, f"解盾后仍失败 ({len(body)}B)"

        if body[:4] == b'%PDF' or len(body) > 1000:
            with open(path, 'wb') as f:
                f.write(body)
            return True, f"{len(body)} B"
        return False, f"无效文件 (Head: {body[:10]}...)"
    except Exception as e:
        return False, str(e)[:50]


async def sse_search_download(json_path: str, data_dir: str):
    """使用 Playwright 引擎下载 SSE 搜索结果中的文件"""
    _require(async_playwright, 'playwright', 'playwright')

    print(f"\n>>> [阶段3] 启动下载引擎 (Playwright)...")
    if not os.path.exists(json_path):
        print("❌ 找不到汇总文件")
        return

    with open(json_path, 'r', encoding='utf-8') as f:
        results = json.load(f)

    files_dir = os.path.join(os.path.dirname(json_path), 'files')
    os.makedirs(files_dir, exist_ok=True)

    tasks = []
    for r in results:
        fpath = os.path.join(files_dir, r['local_filename'])
        if os.path.exists(fpath) and os.path.getsize(fpath) > 3000:
            continue
        if r.get('url'):
            tasks.append((r['url'], fpath, r['local_filename']))

    print(f"    待下载任务: {len(tasks)} (总数: {len(results)})")
    if not tasks:
        print("    ✅ 所有文件已存在，无需下载。")
        return

    failed_list = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled']
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            extra_http_headers={"Referer": "https://www.sse.com.cn/"}
        )

        # 预热
        pg = await context.new_page()
        try:
            await pg.goto('https://www.sse.com.cn/disclosure/listedinfo/announcement/',
                          wait_until='domcontentloaded')
            await asyncio.sleep(3)
        finally:
            await pg.close()

        for i, (url, path, name) in enumerate(tasks):
            print(f"[{i + 1}/{len(tasks)}] {name}", end=" ")
            ok, msg = await _playwright_download_file(context, url, path)
            print(f"✓ {msg}" if ok else f"✗ {msg}")

            if not ok:
                failed_list.append(f"{name} | {url} | {msg}")

            await asyncio.sleep(3 if not ok else random.uniform(0.5, 1.5))

        await browser.close()

    # 写入失败日志
    if failed_list:
        fail_log = os.path.join(data_dir, 'download_failed.txt')
        with open(fail_log, 'w', encoding='utf-8') as f:
            f.write('\n'.join(failed_list))
        print(f"\n⚠️ 有 {len(failed_list)} 个文件下载失败，详情已记录到: {fail_log}")
    else:
        print("\n✅ 所有任务下载完成，无失败记录。")


# ╔══════════════════════════════════════════════════════════════════╗
# ║         模块二：上交所问询函专栏爬虫 (SSE Inquiry)                    ║
# ╚══════════════════════════════════════════════════════════════════╝

class SSEInquiriesScraper:
    """
    通过上交所问询函专栏 API 爬取数据，含多线程下载、核对补录、去重。
    来源: sse_inquiries.py
    """

    def __init__(self, output_dir: str, page_size: int = SSE_INQUIRY_PAGE_SIZE):
        _require(cffi_requests, 'curl_cffi')
        self.output_dir = output_dir
        self.files_dir = os.path.join(output_dir, 'files')
        self.base_url = "https://query.sse.com.cn/commonSoaQuery.do"
        self.site_base = "https://www.sse.com.cn"
        self.page_url = "https://www.sse.com.cn/disclosure/credibility/supervision/inquiries/"
        self.page_size = page_size

        self.session = cffi_requests.Session(impersonate="chrome124")
        self.headers = {
            'Referer': self.page_url,
            'Origin': 'https://www.sse.com.cn',
            'Accept': '*/*',
        }
        self.session.headers.update(self.headers)

        os.makedirs(self.output_dir, exist_ok=True)
        self._init_session()

    def _init_session(self):
        print("[SSE专栏] 初始化会话...")
        try:
            self.session.get(self.page_url, timeout=15)
            time.sleep(0.5)
            params = self._build_params(page_no=1)
            resp = self.session.get(self.base_url, params=params, timeout=15)
            if '"result"' in resp.text:
                print("✅ 初始化成功")
            else:
                print("⚠️ 初始化可能有问题")
        except Exception as e:
            print(f"⚠️ 初始化警告: {e}")

    def _build_params(self, page_no: int = 1, page_size: int = None,
                      stock_code: str = '', start_date: str = '',
                      end_date: str = '') -> dict:
        page_size = page_size or self.page_size
        return {
            'jsonCallBack': f'jsonpCallback{random.randint(10000000, 99999999)}',
            'isPagination': 'true',
            'pageHelp.pageSize': page_size,
            'pageHelp.pageNo': page_no,
            'pageHelp.beginPage': page_no,
            'pageHelp.cacheSize': 1,
            'pageHelp.endPage': page_no,
            'sqlId': 'BS_KCB_GGLL_NEW',
            'siteId': 28,
            'channelId': '10012,10743,10744',
            'type': '4',           # 主板为 4；全部板块为空
            'stockcode': stock_code,
            'extGGDL': '1',        # 问询函为 1；全部类型为空
            'createTime': start_date,
            'createTimeEnd': end_date,
            'order': 'createTime|desc,stockcode|asc',
            '_': int(time.time() * 1000)
        }

    # ---------- 获取总数 ----------
    def get_total_count(self, stock_code: str = '', start_date: str = '',
                        end_date: str = '') -> tuple:
        params = self._build_params(page_no=1, stock_code=stock_code,
                                    start_date=start_date, end_date=end_date)
        try:
            response = self.session.get(self.base_url, params=params, timeout=15)
            data = parse_jsonp(response.text)
            if data:
                page_help = data.get('pageHelp', {})
                return page_help.get('total', 0), page_help.get('pageCount', 0)
        except Exception as e:
            print(f"获取总数失败: {e}")
        return None, None

    # ---------- 全量爬取 ----------
    def search_all(self, stock_code: str = '', start_date: str = '',
                   end_date: str = '', max_pages: int = None) -> list:
        all_results = []
        page = 1
        errors = 0

        total, total_pages = self.get_total_count(stock_code, start_date, end_date)
        if total is None:
            print("无法获取总数")
            return []

        print(f"总记录: {total}, 总页数: {total_pages}")

        while True:
            params = self._build_params(page_no=page, stock_code=stock_code,
                                        start_date=start_date, end_date=end_date)
            try:
                response = self.session.get(self.base_url, params=params, timeout=20)
                data = parse_jsonp(response.text)

                if not data:
                    errors += 1
                    if errors >= 5:
                        break
                    time.sleep(3)
                    continue

                results = data.get('result', [])
                if not results:
                    break

                for item in results:
                    parsed = self._parse_item(item)
                    if parsed:
                        all_results.append(parsed)

                errors = 0
                print(f"第 {page}/{total_pages} 页, 已获取 {len(all_results)} 条")

                if page >= total_pages:
                    print("✅ 全部完成")
                    break
                if max_pages and page >= max_pages:
                    break

                page += 1
                time.sleep(random.uniform(0.8, 1.5))

            except Exception as e:
                errors += 1
                print(f"异常: {e}")
                if errors >= 5:
                    break
                time.sleep(3)

        return all_results

    # ---------- 解析单条 ----------
    def _parse_item(self, item: dict) -> dict:
        if not item:
            return None

        stock_code = item.get('STOCKCODE', item.get('stockcode', ''))
        stock_name = item.get('STOCKNAME', item.get('extGSJC', ''))
        title = item.get('TITLE', item.get('docTitle', ''))
        doc_url = item.get('DOCURL', item.get('docURL', ''))
        create_time = item.get('CREATETIME', item.get('createTime', ''))
        doc_type = item.get('DOCTYPE', item.get('docType', ''))

        if doc_url and not doc_url.startswith('http'):
            if doc_url.startswith('www.'):
                doc_url = f"https://{doc_url}"
            elif doc_url.startswith('/'):
                doc_url = f"{self.site_base}{doc_url}"
            else:
                doc_url = f"{self.site_base}/{doc_url}"

        date_str = str(create_time)[:10].replace('-', '') if create_time else ''
        s_title = safe_filename(title)

        file_ext = 'pdf'
        if doc_url:
            if '.doc' in doc_url.lower():
                file_ext = 'doc'
            elif '.xls' in doc_url.lower():
                file_ext = 'xls'

        url_hash = ""
        if doc_url:
            url_hash = hashlib.md5(doc_url.encode('utf-8')).hexdigest()[:6]

        filename = f"{stock_code}_{date_str}_{s_title}_{url_hash}.{file_ext}"

        return {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'title': title,
            'url': doc_url,
            'create_time': create_time,
            'doc_type': doc_type,
            'local_filename': filename,
        }

    # ---------- 保存结果 ----------
    def save_results(self, results: list) -> str:
        if not results:
            return None

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        fieldnames = ['stock_code', 'stock_name', 'title', 'url',
                      'create_time', 'doc_type', 'local_filename']

        json_path = os.path.join(self.output_dir, f'问询函专栏_{timestamp}.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        csv_path = os.path.join(self.output_dir, f'问询函专栏_{timestamp}.csv')
        save_to_csv(results, csv_path, fieldnames)

        latest_path = os.path.join(self.output_dir, 'latest_results.json')
        with open(latest_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        print(f"保存: {json_path}")
        return json_path

    # ---------- 下载文件（多线程，修复防盗链） ----------
    def download_from_json(self, json_path: str = None, max_workers: int = MAX_DOWNLOAD_WORKERS):
        if json_path is None:
            json_path = os.path.join(self.output_dir, 'latest_results.json')

        if not os.path.exists(json_path):
            print(f"文件不存在: {json_path}")
            return

        with open(json_path, 'r', encoding='utf-8') as f:
            results = json.load(f)

        os.makedirs(self.files_dir, exist_ok=True)

        download_list = [(r['url'], r['local_filename']) for r in results if r.get('url')]
        print(f"下载 {len(download_list)} 个文件到 {self.files_dir}")

        success, skip, fail = 0, 0, 0
        failed_files = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._download_file, url, fn): (url, fn)
                       for url, fn in download_list}
            iterator = (tqdm(as_completed(futures), total=len(futures))
                        if HAS_TQDM else as_completed(futures))

            for future in iterator:
                url, fn = futures[future]
                ok, msg = future.result()
                if ok:
                    if "跳过" in msg:
                        skip += 1
                    else:
                        success += 1
                else:
                    fail += 1
                    failed_files.append(f"{fn}: {msg}")
                time.sleep(0.3)

        print(f"完成: 成功 {success} 跳过 {skip} 失败 {fail}")

        if failed_files:
            fail_log = os.path.join(self.output_dir, 'download_failed.txt')
            with open(fail_log, 'w', encoding='utf-8') as f:
                f.write('\n'.join(failed_files))
            print(f"失败记录: {fail_log}")

    def _download_file(self, url: str, filename: str) -> tuple:
        """下载单个文件（修复版：保留原 Headers，增加重试与超时宽容度）"""
        filepath = os.path.join(self.files_dir, filename)
        if os.path.exists(filepath):
            if os.path.getsize(filepath) > 1000:
                return (True, "跳过")
            else:
                os.remove(filepath)

        max_retries = 3
        headers = {
            'Referer': self.page_url,
            'Origin': 'https://www.sse.com.cn',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,'
                      'image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
        }

        for attempt in range(1, max_retries + 1):
            try:
                download_session = cffi_requests.Session(impersonate="chrome124")

                # 预访问获取 Cookie
                try:
                    download_session.get(self.page_url, headers=headers, timeout=30)
                except Exception:
                    pass

                time.sleep(random.uniform(0.5, 1.5))

                # 正式下载
                resp = download_session.get(url, headers=headers, timeout=60, allow_redirects=True)
                resp.raise_for_status()

                content_type = resp.headers.get('Content-Type', '')
                if 'text/html' in content_type and len(resp.content) < 10000:
                    raise ValueError(f"返回HTML而非文件: {content_type}")

                with open(filepath, 'wb') as f:
                    f.write(resp.content)

                if os.path.getsize(filepath) < 1000:
                    os.remove(filepath)
                    raise ValueError("文件过小")

                return (True, "成功")

            except Exception as e:
                if attempt == max_retries:
                    return (False, f"重试{max_retries}次后失败: {str(e)}")
                time.sleep(2 * attempt)

        return (False, "未知错误")

    # ---------- 从 Excel 链接列下载 ----------
    def download_from_excel(self, excel_path: str, max_workers: int = MAX_DOWNLOAD_WORKERS,
                            col: int = 0):
        _require(pd, 'pandas')

        df = pd.read_excel(excel_path, header=None)
        urls = df.iloc[:, col].dropna().astype(str).str.strip().tolist()

        # 去重（保持顺序）
        seen = set()
        uniq = []
        for u in urls:
            if not u.startswith("http"):
                continue
            if u.startswith("http://"):
                u = "https://" + u[len("http://"):]
            if u not in seen:
                seen.add(u)
                uniq.append(u)

        os.makedirs(self.files_dir, exist_ok=True)

        def make_fn(u: str) -> str:
            u0 = u.split("?", 1)[0]
            base = os.path.basename(u0) or "doc.pdf"
            stem, ext = os.path.splitext(base)
            if not ext:
                ext = ".pdf"
            h = hashlib.md5(u.encode("utf-8")).hexdigest()[:10]
            stem = re.sub(r'[\\/*?:"<>|\r\n]+', "_", stem)[:80] or "doc"
            return f"{stem}_{h}{ext}"

        download_list = [(u, make_fn(u)) for u in uniq]
        print(f"下载 {len(download_list)} 个文件到 {self.files_dir}")

        failed = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(self._download_file, u, fn) for u, fn in download_list]
            for fut, (u, fn) in zip(futs, download_list):
                ok, msg = fut.result()
                if not ok:
                    failed.append(f"{fn}\t{u}\t{msg}")
                time.sleep(0.05)

        if failed:
            fail_log = os.path.join(self.output_dir, "download_failed.txt")
            with open(fail_log, "a", encoding="utf-8") as f:
                f.write("\n".join(failed) + "\n")
            print(f"有失败，见: {fail_log}")

    # ---------- 核对并补录 ----------
    def verify_and_retry(self, json_path: str = None):
        """核对本地文件，更新失败记录，并尝试补录下载"""
        if json_path is None:
            json_path = os.path.join(self.output_dir, 'latest_results.json')

        if not os.path.exists(json_path):
            print(f"找不到数据文件: {json_path}")
            return

        print("\n=== 开始核对本地文件完整性 ===")
        with open(json_path, 'r', encoding='utf-8') as f:
            all_records = json.load(f)

        missing_records = []
        valid_count = 0

        for item in all_records:
            if not item.get('url'):
                continue
            filepath = os.path.join(self.files_dir, item['local_filename'])
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
                valid_count += 1
            else:
                missing_records.append(item)

        print(f"理论总数: {len(all_records)}")
        print(f"本地实存: {valid_count}")
        print(f"缺失/损坏: {len(missing_records)}")

        fail_log = os.path.join(self.output_dir, 'download_failed.txt')

        if missing_records:
            print(f"\n检测到 {len(missing_records)} 个文件缺失，正在更新错误日志...")
            with open(fail_log, 'w', encoding='utf-8') as f:
                for item in missing_records:
                    f.write(f"{item['local_filename']}: {item['url']}\n")
            print(f"已更新: {fail_log}")

            user_input = input(f"\n是否立即尝试下载这 {len(missing_records)} 个缺失文件? (y/n): ")
            if user_input.lower() == 'y':
                print("\n=== 开始补录下载 ===")
                download_list = [(r['url'], r['local_filename']) for r in missing_records]
                success, fail = 0, 0
                use_threads = len(download_list) > 10

                if use_threads:
                    with ThreadPoolExecutor(max_workers=3) as executor:
                        futures = {executor.submit(self._download_file, url, fn): fn
                                   for url, fn in download_list}
                        iterator = (tqdm(as_completed(futures), total=len(futures))
                                    if HAS_TQDM else as_completed(futures))
                        for future in iterator:
                            ok, msg = future.result()
                            if ok:
                                success += 1
                            else:
                                fail += 1
                                print(f"补录失败: {futures[future]} - {msg}")
                else:
                    for url, fn in download_list:
                        print(f"正在补录: {fn[:30]}...", end="")
                        ok, msg = self._download_file(url, fn)
                        if ok:
                            print(" [成功]")
                            success += 1
                        else:
                            print(f" [失败: {msg}]")
                            fail += 1

                print(f"\n补录结束: 成功 {success}, 仍失败 {fail}")

                if success > 0:
                    self.verify_and_retry(json_path)
        else:
            print("\n🎉 恭喜！所有文件已全部下载完成！")
            if os.path.exists(fail_log):
                os.remove(fail_log)
                print("已清除旧的错误日志。")

    # ---------- 文件去重 ----------
    def deduplicate_files(self):
        """根据文件内容 (MD5) 检测并删除重复文件"""
        if not os.path.exists(self.files_dir):
            print("文件夹不存在，无需去重")
            return

        print("\n=== 开始扫描重复文件 (基于内容 MD5) ===")
        files = [f for f in os.listdir(self.files_dir)
                 if os.path.isfile(os.path.join(self.files_dir, f))]
        print(f"扫描目录: {self.files_dir}")
        print(f"文件总数: {len(files)}")

        seen_hashes = {}
        duplicates = []

        iterator = tqdm(files, desc="计算哈希") if HAS_TQDM else files
        for filename in iterator:
            filepath = os.path.join(self.files_dir, filename)
            if os.path.getsize(filepath) < 100:
                continue
            file_hash = calculate_md5(filepath)
            if file_hash in seen_hashes:
                duplicates.append((filename, seen_hashes[file_hash]))
            else:
                seen_hashes[file_hash] = filename

        if not duplicates:
            print("✅ 未发现重复文件。")
            return

        print(f"\n发现 {len(duplicates)} 个重复文件。")
        print(f"示例: {duplicates[0][0]} == {duplicates[0][1]}")

        confirm = input("是否确认删除这些重复文件？(y/n): ")
        if confirm.lower() == 'y':
            deleted_count = 0
            freed_space = 0
            for dup_name, _ in duplicates:
                dup_path = os.path.join(self.files_dir, dup_name)
                try:
                    size = os.path.getsize(dup_path)
                    os.remove(dup_path)
                    deleted_count += 1
                    freed_space += size
                except Exception as e:
                    print(f"删除失败 {dup_name}: {e}")
            print(f"\n清理完成:")
            print(f"- 删除了 {deleted_count} 个文件")
            print(f"- 释放空间 {freed_space / 1024 / 1024:.2f} MB")
        else:
            print("已取消删除。")


# ╔══════════════════════════════════════════════════════════════════╗
# ║           模块三：巨潮资讯网公告下载器 (cninfo)                       ║
# ╚══════════════════════════════════════════════════════════════════╝

class CninfoDownloader:
    """
    从 Excel 文件读取巨潮资讯网公告链接，批量下载 PDF。
    来源: cninfo_crawler.py
    """

    def __init__(self, output_dir: str = CNINFO_OUTPUT_DIR):
        _require(pd, 'pandas', 'pandas openpyxl')
        _require(std_requests, 'requests')

        from pathlib import Path
        from urllib.parse import urlparse, parse_qs
        self._urlparse = urlparse
        self._parse_qs = parse_qs

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'http://www.cninfo.com.cn/',
        }

        self.session = std_requests.Session()
        self.session.headers.update(self.headers)

        self.stats = {'success': 0, 'fail': 0, 'skip': 0}
        self.failed_items = []

    def parse_url(self, url):
        """解析公告 URL，提取 announcementId 等参数"""
        if pd.isna(url) or not url:
            return None
        try:
            params = self._parse_qs(self._urlparse(str(url).strip()).query)
            return {
                'announcementId': params.get('announcementId', [''])[0],
                'announcementTime': params.get('announcementTime', [''])[0],
                'stockCode': params.get('stockCode', [''])[0],
                'orgId': params.get('orgId', [''])[0],
            }
        except Exception:
            return None

    def download_file(self, url, save_path, max_retries=3):
        """下载文件，支持重试"""
        for attempt in range(max_retries):
            try:
                time.sleep(random.uniform(0.8, 1.5))
                response = self.session.get(url, timeout=60, allow_redirects=True)

                if response.status_code == 200:
                    content = response.content
                    if content[:4] == b'%PDF' or len(content) > 5000:
                        with open(save_path, 'wb') as f:
                            f.write(content)
                        if os.path.getsize(save_path) > 1024:
                            return True
                        os.remove(save_path)
                elif response.status_code == 404:
                    return False

            except std_requests.exceptions.Timeout:
                print(f"      超时，重试 {attempt + 1}/{max_retries}")
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"      错误: {e}")
        return False

    def download_one(self, row, index):
        """下载单条公告"""
        url = row.get('样本链接') or row.get('InquiryLink')
        if pd.isna(url) or not url:
            self.stats['skip'] += 1
            return

        params = self.parse_url(url)
        if not params or not params['announcementId']:
            print(f"[{index:04d}] ⚠ 跳过: 无效URL")
            self.stats['skip'] += 1
            return

        stock_code = str(row.get('Symbol', params['stockCode']))
        short_name = str(row.get('ShortName', ''))
        ann_id = params['announcementId']
        ann_time = params['announcementTime']

        filename = f"{stock_code}_{short_name}_{ann_time.replace('-', '')}_{ann_id}.PDF"
        filename = re.sub(r'[\\/:*?"<>|\s]', '_', filename)
        save_path = self.output_dir / filename

        if save_path.exists() and save_path.stat().st_size > 1024:
            print(f"[{index:04d}] ✓ 已存在: {stock_code} {short_name}")
            self.stats['success'] += 1
            return

        print(f"[{index:04d}] ↓ 下载中: {stock_code} {short_name}")

        download_urls = [
            f"http://static.cninfo.com.cn/finalpage/{ann_time}/{ann_id}.PDF",
            f"http://www.cninfo.com.cn/new/announcement/download?bulletinId={ann_id}&realTime=true",
            f"https://static.cninfo.com.cn/finalpage/{ann_time}/{ann_id}.PDF",
        ]

        for dl_url in download_urls:
            if self.download_file(dl_url, save_path):
                print(f"[{index:04d}] ✓ 成功: {filename}")
                self.stats['success'] += 1
                return

        print(f"[{index:04d}] ✗ 失败: {stock_code} {short_name}")
        self.stats['fail'] += 1
        self.failed_items.append({
            'index': index,
            'stock_code': stock_code,
            'short_name': short_name,
            'announcement_id': ann_id,
            'url': url
        })

    def run(self, excel_path, start=0, end=None):
        """批量下载"""
        print("=" * 60)
        print("  巨潮资讯网公告下载器")
        print("=" * 60)
        print(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  文件: {excel_path}")

        df = pd.read_excel(excel_path, sheet_name=3)
        total = len(df)
        end = end or total

        print(f"  总数: {total} 条")
        print(f"  范围: {start} - {end}")
        print(f"  目录: {self.output_dir.absolute()}")
        print("-" * 60)

        start_time = time.time()

        for i in range(start, min(end, total)):
            self.download_one(df.iloc[i].to_dict(), i)

            if (i + 1) % 10 == 0:
                elapsed = time.time() - start_time
                progress = (i + 1 - start) / (end - start) * 100
                print(f"\n--- 进度: {i + 1}/{end} ({progress:.1f}%) 耗时: {elapsed:.0f}s ---\n")
                time.sleep(random.uniform(2, 4))

        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print("  下载完成!")
        print("-" * 60)
        print(f"  成功: {self.stats['success']}")
        print(f"  失败: {self.stats['fail']}")
        print(f"  跳过: {self.stats['skip']}")
        print(f"  耗时: {elapsed:.1f} 秒")

        if self.failed_items:
            failed_path = self.output_dir / "下载失败列表.xlsx"
            pd.DataFrame(self.failed_items).to_excel(failed_path, index=False)
            print(f"\n  失败列表已保存: {failed_path}")

        print("=" * 60)


class CninfoSearchDownloader:
    """
    巨潮资讯网关键词检索器：
    1) 按关键词 + 日期范围检索公告并建立索引（JSON/CSV）
    2) 根据索引下载附件并输出下载报告
    """

    INDEX_FIELDS = [
        'announcement_id',
        'sec_code',
        'sec_name',
        'org_id',
        'announcement_title_raw',
        'announcement_title',
        'announcement_time_ms',
        'announcement_date',
        'adjunct_url',
        'download_url_static',
        'adjunct_type',
        'adjunct_size',
        'keyword',
        'index_created_at',
    ]

    REPORT_FIELDS = [
        'announcement_id',
        'sec_code',
        'sec_name',
        'announcement_date',
        'download_url_static',
        'local_filename',
        'status',
        'error',
        'file_path',
    ]

    def __init__(self, output_dir: str = CNINFO_OUTPUT_DIR):
        _require(pd, 'pandas', 'pandas openpyxl')
        _require(std_requests, 'requests')

        from urllib.parse import urlparse
        self._urlparse = urlparse

        self.base_output = os.path.join(output_dir, 'cninfo_search')
        self.index_dir = os.path.join(self.base_output, 'index')
        self.files_dir = os.path.join(self.base_output, 'files')
        os.makedirs(self.index_dir, exist_ok=True)
        os.makedirs(self.files_dir, exist_ok=True)

        self.search_url = 'https://www.cninfo.com.cn/new/hisAnnouncement/query'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://www.cninfo.com.cn',
            'Referer': 'https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search',
        }
        self.session = std_requests.Session()
        self.session.headers.update(self.headers)

    @staticmethod
    def _strip_html(raw_text: str) -> str:
        txt = re.sub(r'<[^>]+>', '', str(raw_text or ''))
        txt = txt.replace('&nbsp;', ' ').replace('&amp;', '&')
        return txt.strip()

    @staticmethod
    def _to_date_text(timestamp_ms) -> str:
        if timestamp_ms is None:
            return ''
        try:
            t = int(float(timestamp_ms))
            if t > 10**11:
                t = t / 1000
            return datetime.fromtimestamp(t).strftime('%Y-%m-%d')
        except Exception:
            return ''

    @staticmethod
    def _normalize_page_size(page_size: int) -> int:
        try:
            page_size = int(page_size)
        except Exception:
            page_size = 30
        return max(1, min(page_size, 30))

    def _build_payload(self, keyword: str, start_date: str, end_date: str,
                       page_no: int, page_size: int) -> dict:
        return {
            'pageNum': str(page_no),
            'pageSize': str(page_size),
            'column': 'szse',
            'tabName': 'fulltext',
            'plate': '',
            'stock': '',
            'searchkey': keyword,
            'secid': '',
            'category': '',
            'trade': '',
            'seDate': f'{start_date}~{end_date}',
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true',
        }

    def _normalize_record(self, item: dict, keyword: str, index_created_at: str) -> dict:
        announcement_id = str(item.get('announcementId') or '').strip()
        adjunct_url = str(item.get('adjunctUrl') or '').strip()
        if adjunct_url and adjunct_url.startswith('http'):
            download_url_static = adjunct_url
        elif adjunct_url:
            download_url_static = f"https://static.cninfo.com.cn/{adjunct_url.lstrip('/')}"
        else:
            download_url_static = ''

        raw_title = str(item.get('announcementTitle') or '')
        ann_time = item.get('announcementTime')

        return {
            'announcement_id': announcement_id,
            'sec_code': str(item.get('secCode') or '').strip(),
            'sec_name': self._strip_html(item.get('secName') or ''),
            'org_id': str(item.get('orgId') or '').strip(),
            'announcement_title_raw': raw_title,
            'announcement_title': self._strip_html(raw_title),
            'announcement_time_ms': ann_time if ann_time is not None else '',
            'announcement_date': self._to_date_text(ann_time),
            'adjunct_url': adjunct_url,
            'download_url_static': download_url_static,
            'adjunct_type': str(item.get('adjunctType') or '').strip(),
            'adjunct_size': item.get('adjunctSize') if item.get('adjunctSize') is not None else '',
            'keyword': keyword,
            'index_created_at': index_created_at,
        }

    def search_and_build_index(self, keyword: str, start_date: str, end_date: str,
                               page_size: int = 30, max_pages: int = None,
                               max_results: int = None) -> list:
        page_size = self._normalize_page_size(page_size)

        print(f"\n=== [cninfo-search] 开始检索 ===")
        print(f"关键词: {keyword}")
        print(f"范围: {start_date} ~ {end_date}")
        print(f"页大小: {page_size} (接口上限 30)")
        if max_pages:
            print(f"最大页数: {max_pages}")
        if max_results:
            print(f"最大结果数: {max_results}")

        all_records = []
        seen_keys = set()
        page_no = 1
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        while True:
            payload = self._build_payload(keyword, start_date, end_date, page_no, page_size)
            try:
                resp = self.session.post(self.search_url, data=payload, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"❌ 第 {page_no} 页请求失败: {e}")
                break

            announcements = data.get('announcements') or []
            total_announcement = data.get('totalAnnouncement')
            has_more = bool(data.get('hasMore'))

            if not announcements:
                print(f"第 {page_no} 页无数据，结束分页。")
                break

            page_added = 0
            for item in announcements:
                record = self._normalize_record(item, keyword, created_at)
                uniq_key = record.get('announcement_id') or record.get('download_url_static')
                if not uniq_key or uniq_key in seen_keys:
                    continue
                seen_keys.add(uniq_key)
                all_records.append(record)
                page_added += 1
                if max_results and len(all_records) >= max_results:
                    break

            print(f"页 {page_no}: 本页 {len(announcements)} 条, 新增 {page_added} 条, "
                  f"累计 {len(all_records)} 条, 接口总量 {total_announcement}")

            if max_results and len(all_records) >= max_results:
                print(f"达到 max-results={max_results}，停止抓取。")
                break
            if max_pages and page_no >= max_pages:
                print(f"达到 max-pages={max_pages}，停止抓取。")
                break
            if not has_more:
                print("hasMore=False，停止抓取。")
                break

            page_no += 1
            time.sleep(random.uniform(0.5, 1.0))

        return all_records

    def _keyword_tag(self, keyword: str) -> str:
        tag = safe_filename(keyword, max_len=40).strip().replace(' ', '_')
        return tag or 'keyword'

    def save_index(self, keyword: str, records: list) -> dict:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        key_tag = self._keyword_tag(keyword)

        snapshot_json = os.path.join(self.index_dir, f'cninfo_search_{key_tag}_{timestamp}.json')
        snapshot_csv = os.path.join(self.index_dir, f'cninfo_search_{key_tag}_{timestamp}.csv')
        latest_json = os.path.join(self.index_dir, 'latest_index.json')
        latest_csv = os.path.join(self.index_dir, 'latest_index.csv')

        with open(snapshot_json, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        with open(latest_json, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        save_to_csv(records, snapshot_csv, self.INDEX_FIELDS)
        save_to_csv(records, latest_csv, self.INDEX_FIELDS)

        print("\n=== [cninfo-search] 索引已生成 ===")
        print(f"记录数: {len(records)}")
        print(f"快照 JSON: {snapshot_json}")
        print(f"快照 CSV : {snapshot_csv}")
        print(f"最新 JSON: {latest_json}")
        print(f"最新 CSV : {latest_csv}")

        return {
            'snapshot_json': snapshot_json,
            'snapshot_csv': snapshot_csv,
            'latest_json': latest_json,
            'latest_csv': latest_csv,
        }

    def _load_index(self, index_path: str = None) -> tuple:
        index_path = index_path or os.path.join(self.index_dir, 'latest_index.json')
        if not os.path.exists(index_path):
            print(f"❌ 索引文件不存在: {index_path}")
            return [], index_path

        records = []
        try:
            if index_path.lower().endswith('.json'):
                with open(index_path, 'r', encoding='utf-8') as f:
                    records = json.load(f)
            elif index_path.lower().endswith('.csv'):
                df = pd.read_csv(index_path, dtype=str).fillna('')
                records = df.to_dict('records')
            else:
                print(f"❌ 不支持的索引格式: {index_path}")
                return [], index_path
        except Exception as e:
            print(f"❌ 读取索引失败: {e}")
            return [], index_path

        if not isinstance(records, list):
            records = []
        return records, index_path

    def _guess_ext(self, record: dict) -> str:
        adjunct_url = str(record.get('adjunct_url') or '').strip()
        if adjunct_url:
            ext = os.path.splitext(self._urlparse(adjunct_url).path)[1]
            if ext:
                return ext.lower()

        file_type = re.sub(r'[^a-zA-Z0-9]', '', str(record.get('adjunct_type') or '')).lower()
        if file_type:
            return f".{file_type[:8]}"
        return '.pdf'

    def _build_local_filename(self, record: dict) -> str:
        sec_code = safe_filename(record.get('sec_code') or 'unknown', max_len=20) or 'unknown'
        sec_name = safe_filename(record.get('sec_name') or 'unknown', max_len=40) or 'unknown'
        sec_name = sec_name.replace(' ', '_')
        ann_date = re.sub(r'[^0-9]', '', str(record.get('announcement_date') or ''))
        ann_date = ann_date or 'unknown'
        ann_id = safe_filename(record.get('announcement_id') or 'noid', max_len=40) or 'noid'
        ext = self._guess_ext(record)
        return f"{sec_code}_{sec_name}_{ann_date}_{ann_id}{ext}"

    def _download_file(self, url: str, save_path: str, max_retries: int = 3) -> tuple:
        err = ''
        for attempt in range(max_retries):
            try:
                time.sleep(random.uniform(0.5, 1.2))
                response = self.session.get(url, timeout=60, allow_redirects=True)
                if response.status_code == 200:
                    content = response.content
                    ctype = response.headers.get('Content-Type', '').lower()
                    if 'text/html' in ctype and len(content) < 50000 and b'<html' in content[:2000].lower():
                        err = f"返回 HTML ({ctype})"
                    else:
                        with open(save_path, 'wb') as f:
                            f.write(content)
                        if os.path.getsize(save_path) > 1024:
                            return True, "成功"
                        os.remove(save_path)
                        err = "文件过小"
                elif response.status_code in (404, 410):
                    return False, f"HTTP {response.status_code}"
                else:
                    err = f"HTTP {response.status_code}"
            except std_requests.exceptions.Timeout:
                err = f"请求超时({attempt + 1}/{max_retries})"
            except Exception as e:
                err = str(e)

            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
        return False, (err or "未知错误")

    def download_from_index(self, index_path: str = None, max_workers: int = MAX_DOWNLOAD_WORKERS) -> str:
        records, index_path = self._load_index(index_path)
        if not records:
            print("索引为空，跳过下载。")
            return None

        max_workers = max(1, int(max_workers or 1))
        report_rows = []
        download_tasks = []
        seen_keys = set()

        for record in records:
            record = record or {}
            key = str(record.get('announcement_id') or record.get('download_url_static') or '').strip()
            if key and key in seen_keys:
                report_rows.append({
                    'announcement_id': str(record.get('announcement_id') or ''),
                    'sec_code': str(record.get('sec_code') or ''),
                    'sec_name': str(record.get('sec_name') or ''),
                    'announcement_date': str(record.get('announcement_date') or ''),
                    'download_url_static': str(record.get('download_url_static') or ''),
                    'local_filename': '',
                    'status': 'skip',
                    'error': 'duplicate_key',
                    'file_path': '',
                })
                continue
            if key:
                seen_keys.add(key)

            url = str(record.get('download_url_static') or record.get('adjunct_url') or '').strip()
            local_filename = self._build_local_filename(record)
            save_path = os.path.join(self.files_dir, local_filename)

            if not url:
                report_rows.append({
                    'announcement_id': str(record.get('announcement_id') or ''),
                    'sec_code': str(record.get('sec_code') or ''),
                    'sec_name': str(record.get('sec_name') or ''),
                    'announcement_date': str(record.get('announcement_date') or ''),
                    'download_url_static': '',
                    'local_filename': local_filename,
                    'status': 'skip',
                    'error': 'missing_url',
                    'file_path': save_path,
                })
                continue

            if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
                report_rows.append({
                    'announcement_id': str(record.get('announcement_id') or ''),
                    'sec_code': str(record.get('sec_code') or ''),
                    'sec_name': str(record.get('sec_name') or ''),
                    'announcement_date': str(record.get('announcement_date') or ''),
                    'download_url_static': url,
                    'local_filename': local_filename,
                    'status': 'skip',
                    'error': '',
                    'file_path': save_path,
                })
                continue

            download_tasks.append((record, url, local_filename, save_path))

        print("\n=== [cninfo-search] 开始按索引下载 ===")
        print(f"索引文件: {index_path}")
        print(f"总记录数: {len(records)}")
        print(f"待下载: {len(download_tasks)}")
        print(f"线程数: {max_workers}")

        if download_tasks:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._download_file, url, save_path): (record, url, local_filename, save_path)
                    for record, url, local_filename, save_path in download_tasks
                }
                iterator = (tqdm(as_completed(futures), total=len(futures), desc='cninfo下载')
                            if HAS_TQDM else as_completed(futures))

                for future in iterator:
                    record, url, local_filename, save_path = futures[future]
                    ok, msg = future.result()
                    report_rows.append({
                        'announcement_id': str(record.get('announcement_id') or ''),
                        'sec_code': str(record.get('sec_code') or ''),
                        'sec_name': str(record.get('sec_name') or ''),
                        'announcement_date': str(record.get('announcement_date') or ''),
                        'download_url_static': url,
                        'local_filename': local_filename,
                        'status': 'success' if ok else 'fail',
                        'error': '' if ok else msg,
                        'file_path': save_path,
                    })

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(self.base_output, f'download_report_{timestamp}.csv')
        save_to_csv(report_rows, report_path, self.REPORT_FIELDS)

        success = sum(1 for r in report_rows if r['status'] == 'success')
        fail = sum(1 for r in report_rows if r['status'] == 'fail')
        skip = sum(1 for r in report_rows if r['status'] == 'skip')

        print("\n=== [cninfo-search] 下载完成 ===")
        print(f"成功: {success}")
        print(f"失败: {fail}")
        print(f"跳过: {skip}")
        print(f"报告: {report_path}")
        return report_path

    @staticmethod
    def _parse_date(date_text: str) -> datetime:
        return datetime.strptime(date_text, '%Y-%m-%d')

    def run(self, keyword: str, step: str = 'index', start_date: str = None, end_date: str = None,
            page_size: int = 30, max_pages: int = None, max_results: int = None,
            index_path: str = None, workers: int = MAX_DOWNLOAD_WORKERS):
        if step not in ('index', 'download', 'all'):
            raise ValueError(f"不支持的 step: {step}")
        if max_pages is not None and max_pages <= 0:
            raise ValueError("max-pages 必须大于 0")
        if max_results is not None and max_results <= 0:
            raise ValueError("max-results 必须大于 0")
        if workers is not None and workers <= 0:
            raise ValueError("workers 必须大于 0")

        today = datetime.now().strftime('%Y-%m-%d')
        default_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        start_date = start_date or default_start
        end_date = end_date or today

        try:
            st = self._parse_date(start_date)
            ed = self._parse_date(end_date)
        except Exception:
            raise ValueError("日期格式错误，请使用 YYYY-MM-DD")
        if st > ed:
            raise ValueError("start-date 不能晚于 end-date")

        generated = None
        if step in ('index', 'all'):
            records = self.search_and_build_index(
                keyword=keyword,
                start_date=start_date,
                end_date=end_date,
                page_size=page_size,
                max_pages=max_pages,
                max_results=max_results,
            )
            generated = self.save_index(keyword, records)

        if step in ('download', 'all'):
            target_index = index_path
            if step == 'all':
                target_index = target_index or (generated.get('latest_json') if generated else None)
            self.download_from_index(target_index, workers)


# ╔══════════════════════════════════════════════════════════════════╗
# ║                     CLI 命 令 行 入 口                             ║
# ╚══════════════════════════════════════════════════════════════════╝

def cmd_sse_search(args):
    """处理 sse-search 子命令"""
    data_dir = args.output or SSE_SEARCH_OUTPUT_DIR
    keyword = args.keyword
    if not keyword:
        print("❌ sse-search 需要提供关键词，请使用 --keyword <关键词>")
        return
    merged_file = SSE_SEARCH_MERGED_FILE
    step = args.step or 'all'

    # 1. 爬取阶段
    if step in ('all', 'crawl'):
        crawler = SSESearchCrawler(data_dir, keyword=keyword)
        curr_year = datetime.now().year
        year = curr_year
        empty_cnt = 0

        print(f"=== [阶段1] 开始回溯爬取 (从 {curr_year} 开始) ===")
        print(f"    关键词: {keyword}")
        print(f"    策略: 遇到连续 {SSE_SEARCH_MAX_EMPTY_YEARS} 年无数据则停止。\n")

        while True:
            start = f"{year}-01-01"
            end = (f"{year}-12-31" if year != curr_year
                   else datetime.now().strftime("%Y-%m-%d"))

            print(f"--- 正在检查 {year} 年 ---")
            total_found = crawler.run_recursive(start, end)

            if total_found > 0:
                empty_cnt = 0
                print(f"    {year} 年共获取 {total_found} 条数据。")
            else:
                print(f"    {year} 年无数据。")
                empty_cnt += 1

            if empty_cnt >= SSE_SEARCH_MAX_EMPTY_YEARS:
                print(f"\n>>> 连续 {empty_cnt} 年无数据，判定已回溯至尽头。停止爬取。")
                break

            year -= 1
            time.sleep(1)

    # 2. 合并阶段
    merged_path = os.path.join(data_dir, merged_file)
    if step in ('all', 'merge', 'crawl'):
        res_path = sse_search_merge(data_dir, keyword, merged_file)
        if res_path:
            merged_path = res_path

    # 3. 下载阶段
    if step in ('all', 'download'):
        asyncio.run(sse_search_download(merged_path, data_dir))


def cmd_sse_inquiry(args):
    """处理 sse-inquiry 子命令"""
    output_dir = args.output or SSE_INQUIRY_OUTPUT_DIR
    step = args.step or 'crawl'
    json_path = args.json

    scraper = SSEInquiriesScraper(output_dir=output_dir)

    if step == 'test':
        print("\n=== 测试 ===")
        total, pages = scraper.get_total_count()
        print(f"总记录: {total}, 总页数: {pages}")
        if total:
            results = scraper.search_all(max_pages=1)
            print(f"获取: {len(results)} 条")
            if results:
                print(f"示例: [{results[0]['stock_code']}] {results[0]['title'][:40]}...")
                print("\n测试下载第一个文件...")
                scraper.files_dir = os.path.join(scraper.output_dir, 'test_download')
                os.makedirs(scraper.files_dir, exist_ok=True)
                ok, msg = scraper._download_file(results[0]['url'], results[0]['local_filename'])
                print(f"✅ 下载成功: {results[0]['local_filename']}" if ok
                      else f"❌ 下载失败: {msg}")

    elif step == 'crawl':
        print("\n=== 开始爬取问询函专栏 ===\n")
        results = scraper.search_all()
        if results:
            scraper.save_results(results)
            print(f"\n共获取 {len(results)} 条记录")

    elif step == 'download':
        scraper.download_from_json(json_path, MAX_DOWNLOAD_WORKERS)

    elif step == 'verify':
        scraper.verify_and_retry(json_path)

    elif step == 'dedup':
        scraper.deduplicate_files()

    elif step == 'download-excel':
        if not json_path:
            print("❌ 需要指定 Excel 文件路径，用 --json 参数")
            return
        col = args.col if hasattr(args, 'col') else 0
        scraper.download_from_excel(json_path, MAX_DOWNLOAD_WORKERS, col)


def cmd_cninfo(args):
    """处理 cninfo 子命令"""
    excel_path = args.excel_file
    if not os.path.exists(excel_path):
        print(f"❌ 文件不存在: {excel_path}")
        return

    output_dir = args.output or CNINFO_OUTPUT_DIR
    downloader = CninfoDownloader(output_dir=output_dir)
    downloader.run(excel_path, args.start, args.end)


def cmd_cninfo_search(args):
    """处理 cninfo-search 子命令：关键词检索 -> 建索引 -> 下载"""
    output_dir = args.output or CNINFO_OUTPUT_DIR
    crawler = CninfoSearchDownloader(output_dir=output_dir)

    try:
        crawler.run(
            keyword=args.keyword,
            step=args.step,
            start_date=args.start_date,
            end_date=args.end_date,
            page_size=args.page_size,
            max_pages=args.max_pages,
            max_results=args.max_results,
            index_path=args.index,
            workers=args.workers,
        )
    except ValueError as e:
        print(f"❌ 参数错误: {e}")


def cmd_cninfo_excel(args):
    """处理 cninfo-excel 子命令：从 Excel 链接列直接下载"""
    excel_path = args.excel_file
    if not os.path.exists(excel_path):
        print(f"❌ 文件不存在: {excel_path}")
        return

    output_dir = args.output or SSE_INQUIRY_OUTPUT_DIR
    scraper = SSEInquiriesScraper(output_dir=output_dir)
    scraper.download_from_excel(excel_path, MAX_DOWNLOAD_WORKERS, args.col)


def main():
    parser = argparse.ArgumentParser(
        description='统一爬虫工具 — 整合上交所搜索、上交所专栏、巨潮资讯网',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  python unified_crawler.py sse-search --keyword 问询函         # 上交所搜索全自动
  python unified_crawler.py sse-search --keyword 年报 --step crawl  # 仅爬取
  python unified_crawler.py sse-inquiry                         # 问询函专栏全量爬取
  python unified_crawler.py sse-inquiry --step download         # 仅下载
  python unified_crawler.py cninfo sample.xlsx -o ./pdfs        # 巨潮批量下载
  python unified_crawler.py cninfo-search 问询函 --step index   # 巨潮关键词检索并建索引
  python unified_crawler.py cninfo-search 年报 --step all       # 检索并按索引下载
  python unified_crawler.py cninfo-excel links.xlsx --col 0     # 从 Excel 链接列下载
        '''
    )
    subparsers = parser.add_subparsers(dest='command', help='选择数据源')

    # ---- sse-search ----
    p_search = subparsers.add_parser('sse-search', help='上交所全站搜索爬虫')
    p_search.add_argument('--step', choices=['all', 'crawl', 'merge', 'download'],
                          default='all', help='执行阶段 (默认: all)')
    p_search.add_argument('--keyword', required=True, help='搜索关键词（必填）')
    p_search.add_argument('-o', '--output', default=None, help=f'输出目录 (默认: {SSE_SEARCH_OUTPUT_DIR})')

    # ---- sse-inquiry ----
    p_inquiry = subparsers.add_parser('sse-inquiry', help='上交所问询函专栏爬虫')
    p_inquiry.add_argument('--step',
                           choices=['crawl', 'test', 'download', 'verify', 'dedup', 'download-excel'],
                           default='crawl', help='执行步骤 (默认: crawl)')
    p_inquiry.add_argument('--json', default=None, help='指定 JSON 文件路径（用于 download/verify）')
    p_inquiry.add_argument('-o', '--output', default=None, help=f'输出目录 (默认: {SSE_INQUIRY_OUTPUT_DIR})')
    p_inquiry.add_argument('--col', type=int, default=0, help='Excel 中链接所在列索引 (默认: 0)')

    # ---- cninfo ----
    p_cninfo = subparsers.add_parser('cninfo', help='巨潮资讯网公告下载器')
    p_cninfo.add_argument('excel_file', help='Excel 文件路径')
    p_cninfo.add_argument('-o', '--output', default=None, help=f'保存目录 (默认: {CNINFO_OUTPUT_DIR})')
    p_cninfo.add_argument('--start', type=int, default=0, help='起始索引 (默认: 0)')
    p_cninfo.add_argument('--end', type=int, default=None, help='结束索引 (默认: 全部)')

    # ---- cninfo-search ----
    p_csearch = subparsers.add_parser('cninfo-search', help='巨潮关键词检索（建索引 + 下载）')
    p_csearch.add_argument('keyword', help='检索关键词')
    p_csearch.add_argument('--step', choices=['index', 'download', 'all'],
                           default='index', help='执行步骤 (默认: index)')
    p_csearch.add_argument('-o', '--output', default=None, help=f'输出目录 (默认: {CNINFO_OUTPUT_DIR})')
    p_csearch.add_argument('--start-date', default=None, help='开始日期 YYYY-MM-DD (默认: 最近30天)')
    p_csearch.add_argument('--end-date', default=None, help='结束日期 YYYY-MM-DD (默认: 今天)')
    p_csearch.add_argument('--page-size', type=int, default=30, help='检索页大小 (默认: 30, 上限: 30)')
    p_csearch.add_argument('--max-pages', type=int, default=None, help='最大抓取页数 (默认: 不限制)')
    p_csearch.add_argument('--max-results', type=int, default=None, help='最大记录数 (默认: 不限制)')
    p_csearch.add_argument('--index', default=None, help='索引文件路径（download 步骤使用）')
    p_csearch.add_argument('--workers', type=int, default=MAX_DOWNLOAD_WORKERS,
                           help=f'下载线程数 (默认: {MAX_DOWNLOAD_WORKERS})')

    # ---- cninfo-excel ----
    p_cexcel = subparsers.add_parser('cninfo-excel', help='从 Excel 链接列批量下载文件')
    p_cexcel.add_argument('excel_file', help='Excel 文件路径')
    p_cexcel.add_argument('-o', '--output', default=None, help='输出目录')
    p_cexcel.add_argument('--col', type=int, default=0, help='链接所在列索引 (默认: 0)')

    # ---- 解析 ----
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    print("=" * 60)
    print(f"  统一爬虫工具 | 模块: {args.command}")
    print(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    dispatch = {
        'sse-search': cmd_sse_search,
        'sse-inquiry': cmd_sse_inquiry,
        'cninfo': cmd_cninfo,
        'cninfo-search': cmd_cninfo_search,
        'cninfo-excel': cmd_cninfo_excel,
    }
    dispatch[args.command](args)


if __name__ == '__main__':
    main()
