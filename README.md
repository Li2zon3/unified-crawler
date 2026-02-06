# unified-crawler

单文件 Python 爬虫工具，统一入口 `unified_crawler.py`，整合：

1. `sse-search`：上交所全站搜索（按关键词检索、按年回溯、合并、下载）
2. `sse-inquiry`：上交所问询函专栏爬取与下载
3. `cninfo`：巨潮资讯（从 Excel 读取链接批量下载）
4. `cninfo-search`：巨潮资讯关键词检索（建索引 JSON/CSV + 按索引下载 + 下载报告）
5. `cninfo-excel`：从 Excel 某列链接直接下载

## 安装

建议使用 venv，避免出现“装了依赖但脚本找不到”的解释器混用问题：

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## 快速开始

```bash
python unified_crawler.py -h
python unified_crawler.py sse-search -h
python unified_crawler.py cninfo-search -h
```

默认输出目录（可用 `-o/--output` 覆盖）：

- `sse-search`：`output/sse_search`
- `sse-inquiry`：`output/sse_inquiry`
- `cninfo`：`output/cninfo`
- `cninfo-search`：`<output>/cninfo_search/index`、`<output>/cninfo_search/files`

## 命令示例

### `sse-search`（关键词必填）

```bash
python unified_crawler.py sse-search --keyword 问询函
python unified_crawler.py sse-search --keyword 年报 --step crawl
python unified_crawler.py sse-search --keyword 年报 --step merge
python unified_crawler.py sse-search --keyword 年报 --step download
```

### `sse-inquiry`

```bash
python unified_crawler.py sse-inquiry
python unified_crawler.py sse-inquiry --step test
python unified_crawler.py sse-inquiry --step download --json ./output/sse_inquiry/latest_results.json
```

### `cninfo`（从 Excel 下载）

```bash
python unified_crawler.py cninfo sample.xlsx
python unified_crawler.py cninfo sample.xlsx --start 0 --end 10
```

### `cninfo-search`（关键词检索 -> 建索引 -> 下载）

索引输出规则：

- 时间戳快照：`cninfo_search_<关键词>_<timestamp>.json/.csv`
- 最新索引：`latest_index.json/.csv`
- 下载报告：`download_report_<timestamp>.csv`（含 `status/error/file_path`）

常用：

```bash
# 只建索引（默认最近 30 天）
python unified_crawler.py cninfo-search 问询函 --step index

# 一次完成索引 + 下载
python unified_crawler.py cninfo-search 年报 --step all --date 2024-01-01~2024-12-31

# 指定股票（会自动解析为 code,orgId）
python unified_crawler.py cninfo-search 年报 --step index --stock 000001

# 指定分类（中文别名或 category_...）
python unified_crawler.py cninfo-search 年报 --step index --category 年报

# 指定板块（示例：科创板）
python unified_crawler.py cninfo-search 年报 --step index --plate shkcp

# 全市场（包含更多市场，结果可能更大；注意用引号传空字符串）
python unified_crawler.py cninfo-search 年报 --step index --column ""
```

自动分段说明：

- 当接口返回的 `totalRecordNum` 超过 `100 * page_size`（巨潮单次查询页数上限）且你未显式设置 `--max-pages/--max-results` 时，
  脚本会自动按日期范围二分分段检索，尽量避免结果被 100 页上限截断。

### `cninfo-excel`（Excel 某列链接下载）

```bash
python unified_crawler.py cninfo-excel links.xlsx --col 0
```

## 常见问题

- 依赖报错：
  - 确认你运行脚本的 Python 就是安装依赖的那个（推荐 venv）。
  - `playwright` 相关：需要额外执行 `python -m playwright install chromium`。
- 网络/反爬：
  - 上交所/巨潮接口可能存在限流或策略变化，建议降低并发（`--workers`）并重试。

## License

MIT
