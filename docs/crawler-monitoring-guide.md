# 爬虫监控与使用说明

## 1. 概览

这个仓库当前有三套爬虫和一套统一监控内核：

- `bili`: Bilibili 按天回填
- `zhihu`: Zhihu 一年窗流式抓取并实时按天落盘
- `xhs`: Xiaohongshu 按发布时间分桶
- `crawl_monitor.py` / `monitor_core.py`: 统一 monitor

统一 monitor 负责：

- 启动 runner 子进程
- 写 `status.json`
- 写 `latest_run.json`
- 检查 `checkpoint.json` 是否推进
- 卡死自动重启
- 连续启动失败后自动尝试 `disable_cdp`

## 2. 目录结构

### 根目录

- `README.md`: 顶层说明
- `docs/crawler-monitoring-guide.md`: 本文档
- `artifacts/`: 运行状态、日志和爬取结果
- `scripts/`: 监控入口、runner、PowerShell 包装脚本
- `MediaCrawler/`: crawler 核心实现

### `scripts/`

- `crawl_monitor.py`: 统一 CLI
- `monitor_core.py`: monitor 状态机和进程管理
- `run_bili_ai_time_range_job.py`: Bilibili runner
- `run_zhihu_time_range_job.py`: Zhihu runner
- `run_xhs_time_range_job.py`: Xiaohongshu runner
- `start_bili_ai_monitor.ps1`: Bilibili PowerShell 入口
- `start_zhihu_monitor.ps1`: Zhihu PowerShell 入口
- `start_xhs_monitor.ps1`: Xiaohongshu PowerShell 入口

### `artifacts/`

每个平台各有一个运行时目录：

- `artifacts/ai_crawl_monitor/`
- `artifacts/zhihu_crawl_monitor/`
- `artifacts/xhs_crawl_monitor/`

每个平台各有一个数据目录：

- `artifacts/ai_crawl_data/bili/json/`
- `artifacts/ai_crawl_data/zhihu/json/`
- `artifacts/ai_crawl_data/xhs/json/`

### 运行时文件

- `status.json`: 对外状态快照
- `checkpoint.json`: 续跑状态
- `monitor.log`: monitor 聚合日志
- `latest_run.json`: 最近一次 run 的摘要
- `runs/<run_id>/crawler.log`: 某次 child run 的独立日志

## 3. 日分桶规则

### Bilibili

- 使用绝对时间范围按天抓取
- 内容、评论、创作者文件都会按天命名
- 输出文件名示例：
  - `search_contents_2026-03-01.json`
  - `search_comments_2026-03-01.json`
  - `search_creators_2026-03-01.json`

### Zhihu

- 使用 `sort=created_time + time_interval=a_year`
- 从最新到最旧顺序翻页
- 每条内容按真实发布日期写入对应日文件
- 每条内容的评论也写入该内容发布日期的同一天文件
- 不创建空文件

### Xiaohongshu

- 搜索结果先拿 note，再拿详情
- 详情中的发布时间决定写入哪一天的文件
- 不创建空文件

## 4. 用法

### 推荐用法：平台包装脚本

#### Bilibili

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action start
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action status
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action stop
```

#### Zhihu

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_zhihu_monitor.ps1 -Action start
powershell -ExecutionPolicy Bypass -File .\scripts\start_zhihu_monitor.ps1 -Action status
powershell -ExecutionPolicy Bypass -File .\scripts\start_zhihu_monitor.ps1 -Action stop
```

#### Xiaohongshu

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_xhs_monitor.ps1 -Action start
powershell -ExecutionPolicy Bypass -File .\scripts\start_xhs_monitor.ps1 -Action status
powershell -ExecutionPolicy Bypass -File .\scripts\start_xhs_monitor.ps1 -Action stop
```

### 统一入口

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py start --platform bili --continuous-backfill
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py status --platform bili --json
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py stop --platform bili
```

Zhihu 默认是新流式模式，也可以显式指定：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py start --platform zhihu --search-mode one_year_stream_bucketed --continuous-backfill --disable-cdp --headless
```

如果要回退到旧的逐天模式：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\run_zhihu_time_range_job.py --search-mode daily_limit_in_time_range --keyword ai --start-day 2026-03-26 --end-day 2026-03-26
```

### 直接跑 runner

只建议在排障时使用：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\run_bili_ai_time_range_job.py --keyword ai --start-day 2026-03-26 --end-day 2026-03-26 --continuous-backfill
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\run_zhihu_time_range_job.py --keyword ai --continuous-backfill --search-mode one_year_stream_bucketed --disable-cdp --headless
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\run_xhs_time_range_job.py --keyword ai --start-day 2026-03-26 --end-day 2026-03-26 --continuous-backfill
```

## 5. 状态文件怎么看

`status.json` 常用字段：

- `monitor_state`: monitor 状态，常见值有 `running`、`restarting`、`completed`、`stopped`、`error`
- `job_state`: checkpoint 对应的业务状态
- `child_pid`: 当前 runner 进程
- `run_id`: 当前或最近一次 run 的目录名
- `restart_count`: monitor 已重启次数
- `consecutive_failures`: 连续失败次数
- `current_day`: 当前日桶，Zhihu 流式模式里表示 `last_emitted_day`
- `resume_page`: 下次恢复时继续或回放的页号
- `latest_crawler_log`: 当前 run 的日志

Zhihu 流式模式新增字段：

- `pages_completed`
- `contents_emitted`
- `comments_emitted`
- `last_emitted_day`
- `last_result_created_day`

## 6. 常见错误

### 6.1 `Playwright browser bootstrap was denied by the current environment`

含义：

- 当前不是正常本机桌面会话
- Playwright 子进程或管道被限制

处理：

- 不要在受限代理环境里跑
- 改为本机 PowerShell 直接启动

### 6.2 Bilibili 二维码没出现或扫码超时

现象：

- 日志中出现：
  - `qrcode was not found on the page`
  - `login was not confirmed before timeout`

处理：

- 确认页面能正常打开登录弹窗
- 重新启动 Bilibili monitor
- 必要时先在浏览器里手动登录一次

说明：

- 代码已经改成登录失败抛异常，不会再把失败误判成正常完成
- monitor 也会检查 `checkpoint.state`，避免 child `exit 0` 但业务并未完成时被当成 `completed`

### 6.3 Zhihu `RetryError` / `ConnectError`

现象：

- `failure_reason` 出现 `RetryError`、`ConnectError`

处理：

- 这通常是网络波动或接口瞬时失败
- monitor 会按 checkpoint 自动重启
- Zhihu 流式模式会从 `resume_page - 3` 回放重叠页，并依赖内容 ID 去重

### 6.4 Xiaohongshu 一直停在扫码等待

现象：

- 日志里反复出现 `waiting for scan code login`

处理：

- 当前账号没有完成登录确认
- 如果暂时不打算登录，直接 `stop`
- 不建议让 monitor 长时间反复拉起等待扫码

### 6.5 `checkpoint_stalled`

含义：

- 日志可能还在动，但 checkpoint 长时间不推进

处理：

- monitor 会自动重启
- 排查站点登录态、网络、页面结构是否变化

### 6.6 产物污染

典型原因：

- 同一平台同时起了两套 monitor
- 手动 direct runner 和 monitor 同时写同一目录

处理：

- 同一平台只保留一套 monitor
- 统一使用 `start/status/stop`
- 重启前先确认旧 monitor 已经停止

## 7. 目前的已知设计

- Bilibili 和 Xiaohongshu 是按天回填 runner
- Zhihu 是一年窗流式抓取，不再按天发起搜索请求
- Zhihu 旧逐天模式仍可显式启用，但不再是默认模式
- monitor 的 stall 判断优先看 checkpoint 是否推进

## 8. 维护建议

- 不要手工编辑 `checkpoint.json`，除非明确知道恢复语义
- 排障优先看 `runs/<run_id>/crawler.log`
- `monitor.log` 只适合看重启和状态机事件
- 如果 child 已经停止，但 `checkpoint.state` 还不是终态，优先怀疑登录失败、环境失败或异常 clean exit
