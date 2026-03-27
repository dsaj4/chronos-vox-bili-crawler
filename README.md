# Chronos Vox Multi-Platform Crawler

这个仓库目前包含三套按天分桶的增量爬虫：

- `bili`: Bilibili
- `zhihu`: Zhihu
- `xhs`: Xiaohongshu

以及一套统一的本机 monitor 内核，用来负责：

- 子进程拉起和停止
- checkpoint 续跑
- 卡死检测
- 日志隔离
- `status/start/run/stop` 统一接口

详细说明见 [docs/crawler-monitoring-guide.md](docs/crawler-monitoring-guide.md)。

## 快速开始

PowerShell 下推荐直接使用平台脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action start
powershell -ExecutionPolicy Bypass -File .\scripts\start_zhihu_monitor.ps1 -Action start
powershell -ExecutionPolicy Bypass -File .\scripts\start_xhs_monitor.ps1 -Action start
```

查询状态：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action status
powershell -ExecutionPolicy Bypass -File .\scripts\start_zhihu_monitor.ps1 -Action status
powershell -ExecutionPolicy Bypass -File .\scripts\start_xhs_monitor.ps1 -Action status
```

统一入口也可以直接使用：

```powershell
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py status --platform bili --json
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py status --platform zhihu --json
.\MediaCrawler\.venv\Scripts\python.exe .\scripts\crawl_monitor.py status --platform xhs --json
```

## 目录

```text
MediaCrawler/
  media_platform/
  store/
  tools/
scripts/
artifacts/
docs/
```

关键脚本：

- `scripts/crawl_monitor.py`: 统一 monitor CLI
- `scripts/monitor_core.py`: 统一 monitor 内核
- `scripts/run_bili_ai_time_range_job.py`: Bilibili runner
- `scripts/run_zhihu_time_range_job.py`: Zhihu runner
- `scripts/run_xhs_time_range_job.py`: Xiaohongshu runner

## 产物

默认产物目录：

```text
artifacts/ai_crawl_data/<platform>/json/
```

运行时目录：

```text
artifacts/<platform>_crawl_monitor/
```

其中包含：

- `status.json`
- `checkpoint.json`
- `monitor.log`
- `latest_run.json`
- `runs/<run_id>/crawler.log`

## 当前实现要点

- Bilibili: 绝对时间范围按天回填，内容、评论、创作者按天分桶
- Zhihu: 一年窗流式搜索，按内容真实发布日期实时分桶
- Xiaohongshu: 搜索结果抓详情后按内容发布时间做客户端日分桶

## 注意

- 本仓库默认是本机长期运行方案，不是分布式调度方案
- `artifacts/` 下的运行时文件和数据文件不会自动清理
- 如果同一平台同时起两套 monitor，会污染同一份产物目录，所以请只通过 monitor 脚本启动
