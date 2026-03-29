# Agent 自动化与关键词轮转说明

这份文档面向需要长期巡检本仓库的 agent 或调度器。目标是给出稳定入口、明确判定标准，以及自动修复和切词的边界。

## 1. 推荐入口

### `scripts/agent_watch.py`

职责：

- 统一读取 monitor 状态、checkpoint、latest run、日志和产物
- 给出稳定字段：
  - `issue_class`
  - `issue_subclass`
  - `action`
  - `completion_verdict`
  - `artifact_health`
  - `login_required`
- 在 `repair` 模式下优先做无代码修复

命令：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\agent_watch.py audit --platform all --json
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\agent_watch.py repair --platform bili --json
```

exit code：

- `0`：无动作
- `10`：需要登录
- `20`：项目问题，可自动修复或可由 agent 自动修源码
- `30`：高风险项目问题，需要人工关注
- `40`：当前关键词确认完成，可切词

### `scripts/export_keyword_run_summary.py`

职责：

- 导出当前关键词的 JSON + Markdown 摘要
- 复用 `agent_watch audit` 的完成判定

命令：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\export_keyword_run_summary.py --platform zhihu --json
```

默认输出：

- `artifacts/agent_ops/summaries/<platform>/*.json`
- `artifacts/agent_ops/summaries/<platform>/*.md`

### `scripts/keyword_queue.py`

职责：

- 读取共享关键词表
- 管理每个平台的独立关键词状态
- 在确认完成后推进到下一个关键词

命令：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\keyword_queue.py status --platform all --json
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\keyword_queue.py advance --platform zhihu --start-next --json
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\keyword_queue.py restart-current --platform bili --json
```

### `scripts/agent_watchdog.py`

职责：

- 串联 `audit -> repair -> summary -> advance`
- 把项目问题和登录问题分流
- 为外部 agent、Windows 计划任务或其他调度器提供单一稳定入口

命令：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\agent_watchdog.py run-once --platform all --json
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\agent_watchdog.py run-loop --platform all --interval-seconds 900 --json
```

PowerShell 包装：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_agent_watchdog.ps1 -Action once
powershell -ExecutionPolicy Bypass -File .\scripts\start_agent_watchdog.ps1 -Action loop -IntervalSeconds 900
```

watchdog 运行结果会写到：

- `artifacts/agent_ops/watchdog/latest_cycle.json`
- `artifacts/agent_ops/watchdog/history/*.json`

## 2. 共享关键词表与平台独立状态

### 关键词表

文件：`config/keyword_queue.json`

示例：

```json
{
  "version": 1,
  "keywords": [
    { "keyword": "ai", "enabled": true }
  ]
}
```

### 平台状态

文件：`artifacts/agent_ops/keyword_state.json`

每个平台至少记录：

- `current_index`
- `current_keyword`
- `run_state`
- `completion_verdict`
- `last_summary_path`
- `last_started_at`
- `last_completed_at`
- `blocked_reason`

## 3. 巡检流程

每次巡检按以下顺序：

1. `agent_watch audit --platform all --json`
2. 对 `issue_class=login`
   - 不自动重试
   - 不推进关键词
   - 明确提示用户执行登录命令
3. 对 `issue_class=project`
   - 先跑 `agent_watch repair`
   - 如果仍是 `source_bug`
     - 工作区干净：允许 agent 自动修源码
     - 工作区不干净：先汇报，再人工决定是否继续
4. 对 `completion_verdict in {completed, completed_no_results}`
   - 先导出单关键词摘要
   - 再执行 `keyword_queue advance --start-next`

`agent_watchdog.py` 已经把上面流程串好了。

## 4. 完成、中断、登录阻塞的判定

### 真正完成

必须同时满足：

- `monitor_state=completed`
- `checkpoint.state=completed`
- 最新 run 日志没有晚于完成时间的 fatal 错误
- 产物抽查通过

### 中断

任一条件成立即可视为中断：

- `monitor_state=stopped/error` 且 `checkpoint.state` 不是终态
- `child_exit_code != 0`
- heartbeat / progress 不再推进，但不是登录阻塞

### 登录阻塞

以下都视为登录问题：

- `failure_class=login_required`
- `failure_class=account_permission_denied`
- 最新日志出现扫码等待、二维码缺失、登录超时、无权限访问

## 5. 产物抽查规则

主目录：`artifacts/ai_crawl_data/<platform>/json`

兼容检查目录：

- `MediaCrawler/data/<platform>/json`

巡检规则：

- 最新 bucket 文件必须能被 Python `json` 解析
- 样本记录必须包含平台必需字段
- 内容 bucket 的文件日期必须和样本时间字段一致
- 评论 bucket 必须有对应的内容 bucket
- 运行中时，如果 heartbeat 推进但 bucket 的 `mtime / size / record_count` 完全不变，标记为 `degraded`
- 如果 `artifacts/...` 与 `MediaCrawler/data/...` 同时有效落盘，标记为 `config_drift`

平台时间字段：

- `bili` 内容：`create_time`
- `zhihu` 内容：`created_time`
- `xhs` 内容：`time`

说明：

- 评论不按评论自己的发布时间校验，只按所属内容发布日期校验
- Zhihu 是稀疏日期分桶，没有某一天的文件不算异常

## 6. 自动修复边界

### 会自动处理的

- stale `stop.flag`
- monitor 异常退出后的重启
- `browser_or_network` 类故障时切 `disable_cdp`
- 输出目录漂移后恢复到 `artifacts/ai_crawl_data`

### 不自动重试的

- 登录问题
- 账号权限不足

### 需要源码修复的

当 `issue_subclass=source_bug`：

- 工作区干净：允许 agent 自动修源码并重启相关平台
- 工作区不干净：暂停源码修复，先汇报冲突风险

## 7. 登录命令

### Bilibili

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action foreground -Keyword ai
```

### Zhihu

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_zhihu_monitor.ps1 -Action foreground -Keyword ai
```

### Xiaohongshu

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_xhs_monitor.ps1 -Action foreground -Keyword ai
```

## 8. 15 分钟巡检建议

如果你要本机精确按 15 分钟执行，直接用：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_agent_watchdog.ps1 -Action loop -IntervalSeconds 900
```

如果你要外部 agent 或调度器接入，建议只调用：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\agent_watchdog.py run-once --platform all --json
```

这样外部系统不用理解仓库内部细节，只处理 watchdog 返回的稳定 JSON。
