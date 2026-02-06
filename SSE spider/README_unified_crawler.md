# Unified Crawler 使用说明

`unified_crawler.py` 是一个统一入口脚本，整合了 5 个能力：

1. `sse-search`：上交所全站搜索（按关键词检索、回溯、下载）
2. `sse-inquiry`：上交所问询函专栏爬取与下载
3. `cninfo`：巨潮资讯（从 Excel 读取链接批量下载）
4. `cninfo-search`：巨潮资讯关键词检索，建立索引并按索引下载
5. `cninfo-excel`：从 Excel 某列链接直接下载

## 1. 安装依赖

```bash
pip install curl_cffi playwright pandas openpyxl tqdm requests
playwright install chromium
```

## 2. 运行方式

在脚本所在目录执行：

```bash
python unified_crawler.py -h
```

默认输出目录（可用 `-o/--output` 覆盖）：

- `sse-search`：`output/sse_search`
- `sse-inquiry`：`output/sse_inquiry`
- `cninfo`：`output/cninfo`
- `cninfo-search`：`<output>/cninfo_search/index`、`<output>/cninfo_search/files`

## 3. 主要命令

### 3.1 `sse-search`（关键词必填）

```bash
python unified_crawler.py sse-search --keyword 问询函
python unified_crawler.py sse-search --keyword 年报 --step crawl
python unified_crawler.py sse-search --keyword 年报 --step merge
python unified_crawler.py sse-search --keyword 年报 --step download
```

参数：

- `--keyword`：检索关键词（必填）
- `--step`：`all|crawl|merge|download`，默认 `all`
- `-o/--output`：输出目录

### 3.2 `sse-inquiry`

```bash
python unified_crawler.py sse-inquiry
python unified_crawler.py sse-inquiry --step test
python unified_crawler.py sse-inquiry --step download --json ./output/sse_inquiry/latest_results.json
```

参数：

- `--step`：`crawl|test|download|verify|dedup|download-excel`
- `--json`：用于 `download/verify` 的 JSON 路径；用于 `download-excel` 的 Excel 路径
- `--col`：`download-excel` 时链接列索引
- `-o/--output`：输出目录

### 3.3 `cninfo`（从 Excel 下载）

```bash
python unified_crawler.py cninfo sample.xlsx
python unified_crawler.py cninfo sample.xlsx --start 0 --end 10
```

参数：

- `excel_file`：Excel 文件（必填）
- `--start` / `--end`：下载索引范围
- `-o/--output`：输出目录

### 3.4 `cninfo-search`（关键词检索 -> 建索引 -> 下载）

```bash
# 只建索引（默认最近30天）
python unified_crawler.py cninfo-search 问询函 --step index

# 按 latest 索引下载
python unified_crawler.py cninfo-search 问询函 --step download

# 一次完成索引+下载
python unified_crawler.py cninfo-search 年报 --step all --start-date 2026-01-01 --end-date 2026-02-06
```

参数：

- `keyword`：检索关键词（必填）
- `--step`：`index|download|all`，默认 `index`
- `--start-date` / `--end-date`：日期范围，格式 `YYYY-MM-DD`（默认最近 30 天到今天）
- `--page-size`：页大小（最大 30）
- `--max-pages`：最多抓取多少页
- `--max-results`：最多抓取多少条
- `--index`：下载步骤读取的索引文件（JSON/CSV）
- `--workers`：下载并发线程数
- `-o/--output`：输出目录

索引输出规则：

- 时间戳快照：`cninfo_search_<关键词>_<timestamp>.json/.csv`
- 最新索引：`latest_index.json/.csv`

下载报告：

- `download_report_<timestamp>.csv`
- 关键字段：`status`、`error`、`file_path`

### 3.5 `cninfo-excel`（Excel 某列链接下载）

```bash
python unified_crawler.py cninfo-excel links.xlsx --col 0
```

## 4. 发布给他人时的注意事项

1. 脚本已移除个人本地绝对路径默认值，使用者只需按需传 `--output`。
2. 关键词相关流程（`sse-search`、`cninfo-search`）均由用户输入关键词。
3. 若命令失败，优先检查：
   - Python 环境依赖是否安装在当前解释器
   - 网络是否可访问上交所/巨潮
   - Playwright 浏览器是否已安装

## 5. 快速自检

```bash
python unified_crawler.py -h
python unified_crawler.py sse-search -h
python unified_crawler.py cninfo-search -h
```

如果以上帮助命令能正常输出，说明脚本和 CLI 参数已就绪。
