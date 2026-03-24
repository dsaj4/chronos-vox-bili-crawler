# Chronos Vox Bili Crawler

This repository extracts the Bilibili crawler portion from the larger Chronos Vox workspace.

It includes:

- `MediaCrawler/`: crawler core and storage implementation
- `scripts/run_bili_ai_time_range_job.py`: continuous backfill runner
- `scripts/bili_ai_time_range_monitor.py`: watchdog and auto-restart monitor
- `scripts/start_bili_ai_monitor.ps1`: one-click start/stop/status wrapper

## Current defaults

- Keyword: `ai`
- Platform: `bili`
- Continuous backfill: enabled
- Max videos per day: `5`
- Max comments per video: `500`
- Max sub-comments per video: `50`

## Layout

```text
MediaCrawler/
scripts/
```

The scripts expect `MediaCrawler` and `scripts` to be sibling folders at repo root.

## Quick start

From PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action start
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action status
powershell -ExecutionPolicy Bypass -File .\scripts\start_bili_ai_monitor.ps1 -Action stop
```

## Notes

- Runtime data is written under `artifacts/` when the monitor runs.
- This export intentionally excludes local runtime data, browser profiles, and virtual environments.

## Chronos-Vox handoff

When you want to hand a completed crawl to Chronos-Vox, export a crawl result manifest first:

```powershell
python .\scripts\export_crawl_result_manifest.py `
  --output .\artifacts\ai_crawl_monitor\crawl_result_manifest.json
```

The manifest is a stable file-system contract for the Chronos-Vox ingest bridge. The main workspace consumes it and derives the normalized batch, analysis job, and workspace session from that file.
