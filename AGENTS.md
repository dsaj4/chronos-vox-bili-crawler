# Chronos Vox Agent Guide

这份文档是给新接手本仓库的 agent 的最短入口。先看这里，再看详细文档 [docs/agent-automation-guide.md](/C:/Users/dev/Project/爬虫/chronos-vox-bili-crawler/docs/agent-automation-guide.md)。

## 项目原理

运行链路固定为：

`monitor -> runner -> crawler -> artifacts`

- `scripts/crawl_monitor.py`
  - 统一 monitor 入口
  - 提供 `start / run / status / stop`
  - 管理 child 生命周期、重启、状态文件、run 级日志
- `scripts/run_*_time_range_job.py`
  - 平台 runner
  - 管理 checkpoint、断点续传、按天分桶或流式分桶
- `MediaCrawler/media_platform/*/core.py`
  - 平台 crawler 核心逻辑
- `artifacts/`
  - monitor 状态、checkpoint、日志、产物

## 三个平台现状

- `bili`
  - 按绝对日期回填
  - 内容、评论、创作者按天落盘
- `zhihu`
  - 默认使用 `one_year_stream_bucketed`
  - 搜索固定为“近一年 + created_time”，按真实发布日期实时落盘
- `xhs`
  - 搜索结果先拿 note，再抓详情和评论
  - 最终按 note 真实发布日期落盘

## 启动和状态命令

建议优先使用 PowerShell 包装脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action start
powershell -ExecutionPolicy Bypass -File .\scripts\start_zhihu_monitor.ps1 -Action start
powershell -ExecutionPolicy Bypass -File .\scripts\start_xhs_monitor.ps1 -Action start
```

统一查状态：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py status --platform bili --json
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py status --platform zhihu --json
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py status --platform xhs --json
```

## Agent 入口

优先使用以下脚本，不要重新发明状态判定逻辑：

- `scripts/agent_watch.py`
  - `audit --platform {bili,zhihu,xhs,all} --json`
  - `repair --platform {bili,zhihu,xhs,all} --json`
- `scripts/export_keyword_run_summary.py`
  - 导出单关键词运行摘要
- `scripts/keyword_queue.py`
  - 管理共享关键词表和各平台独立进度
- `scripts/agent_watchdog.py`
  - 串联 audit / repair / summary / advance
  - `run-once` 适合外部 agent 或 CI 调用
  - `run-loop --interval-seconds 900` 适合本机长期运行

PowerShell 包装：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_agent_watchdog.ps1 -Action once
powershell -ExecutionPolicy Bypass -File .\scripts\start_agent_watchdog.ps1 -Action loop -IntervalSeconds 900
```

## 关键状态文件

- `artifacts/<platform>_crawl_monitor/status.json`
- `artifacts/<platform>_crawl_monitor/checkpoint.json`
- `artifacts/<platform>_crawl_monitor/latest_run.json`
- `artifacts/<platform>_crawl_monitor/monitor.log`
- `artifacts/<platform>_crawl_monitor/runs/<run_id>/crawler.log`
- `artifacts/agent_ops/latest_audit.json`
- `artifacts/agent_ops/keyword_state.json`
- `artifacts/agent_ops/watchdog/latest_cycle.json`
- `config/keyword_queue.json`

## 判定标准

### 正常运行

满足以下条件时，视为 `running`：

- `status.monitor_state` 在 `starting / running / restarting`
- 最近 `progress_heartbeat_at` 或 `last_progress_at` 仍在推进
- 没有命中登录阻塞

### 真正完成

只有同时满足以下条件，才视为关键词真正跑完：

- `monitor_state=completed`
- `checkpoint.state=completed`
- 最新 run 日志没有后续 fatal 错误
- 最新产物可正常解析，且抽样字段合法

### 登录问题

以下都算登录问题，不自动重试：

- 二维码未出现
- 扫码超时
- 当前账号无权限访问
- 日志或状态明确提示 `login_required` / `account_permission_denied`

命中后：

- 停止继续自动重启
- 提示用户登录
- 保持当前关键词，不切换到下一个关键词

### 项目问题

以下都算项目问题：

- traceback / import / syntax / attribute / type 等代码异常
- 持续 crash
- 输出目录漂移
- 产物损坏或 JSON 无法解析
- monitor / checkpoint 状态机不一致
- 非登录导致的无进度或异常退出

项目问题优先自动修复；如果工作区存在可能冲突的未提交改动，先汇报再改源码。

## 产物规则

- 主产物目录：`artifacts/ai_crawl_data/<platform>/json`
- 文件按天命名：
  - `search_contents_YYYY-MM-DD.json`
  - `search_comments_YYYY-MM-DD.json`
  - `search_creators_YYYY-MM-DD.json` 仅 B 站
- 抽查最少检查：
  - JSON 可解析
  - 样本包含平台必需字段
  - 内容 bucket 日期和内容时间字段一致
  - 评论 bucket 对应的内容 bucket 存在

特殊说明：

- Zhihu 不要求每天都有文件，只要求“有数据的内容进了正确日期文件”
- XHS 需要同时关注 `artifacts/...` 和 `MediaCrawler/data/...`，如果双写则视为 `config_drift`

## 关键词规则

- 关键词表共用：`config/keyword_queue.json`
- 平台状态独立：`artifacts/agent_ops/keyword_state.json`
- 每个平台维护：
  - `current_index`
  - `current_keyword`
  - `completion_verdict`
  - `blocked_reason`

切词原则：

- 只有 `completed` 或 `completed_no_results` 才能切到下一个关键词
- `interrupted` 先恢复当前关键词
- `blocked_by_login` 不切词，先找用户登录

## 重要约束

- 同一平台同一时间只能保留一套 monitor
- 不要同时用 direct runner 和 monitor 写同一个目录
- 不要手改 checkpoint，除非明确知道恢复语义
- 产物抽查和摘要统一用 Python `utf-8` 读取，不依赖 PowerShell `ConvertFrom-Json`
