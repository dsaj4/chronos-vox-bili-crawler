param(
  [ValidateSet("start", "status", "stop", "foreground")]
  [string]$Action = "start",
  [string]$Keyword = "ai",
  [string]$StartDay = (Get-Date -Format "yyyy-MM-dd"),
  [string]$EndDay = (Get-Date -Format "yyyy-MM-dd"),
  [string]$OldestDay = "2009-06-26",
  [int]$CheckIntervalSeconds = 30,
  [int]$StallTimeoutSeconds = 300,
  [switch]$Headless
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root "MediaCrawler\.venv\Scripts\python.exe"
$monitorScript = Join-Path $root "scripts\bili_ai_time_range_monitor.py"
$statusPath = Join-Path $root "artifacts\ai_crawl_monitor\status.json"

if (-not (Test-Path $python)) {
  throw "MediaCrawler virtualenv python not found: $python"
}

if (-not (Test-Path $monitorScript)) {
  throw "Monitor script not found: $monitorScript"
}

function Get-Status {
  $raw = & $python $monitorScript status --json
  if (-not $raw) {
    return $null
  }
  return $raw | ConvertFrom-Json
}

$commonArgs = @(
  $monitorScript,
  "run",
  "--keyword", $Keyword,
  "--start-day", $StartDay,
  "--end-day", $EndDay,
  "--oldest-day", $OldestDay,
  "--continuous-backfill",
  "--check-interval-seconds", "$CheckIntervalSeconds",
  "--stall-timeout-seconds", "$StallTimeoutSeconds"
)

if ($Headless) {
  $commonArgs += "--headless"
}

switch ($Action) {
  "foreground" {
    & $python @commonArgs
    exit $LASTEXITCODE
  }
  "status" {
    & $python $monitorScript status --json
    exit $LASTEXITCODE
  }
  "stop" {
    & $python $monitorScript stop
    exit $LASTEXITCODE
  }
  "start" {
    $status = Get-Status
    if ($status -and $status.monitor_state -in @("starting", "running", "restarting")) {
      Write-Host "Monitor already running."
      if (Test-Path $statusPath) {
        Get-Content $statusPath
      }
      exit 0
    }

    $process = Start-Process $python `
      -ArgumentList $commonArgs `
      -WorkingDirectory $root `
      -PassThru

    Start-Sleep -Seconds 2
    $status = Get-Status
    $displayPid = if ($status -and $status.monitor_pid) { $status.monitor_pid } else { $process.Id }

    Write-Host ""
    Write-Host "Bilibili ai crawl monitor started."
    Write-Host "Monitor PID: $displayPid"
    Write-Host "Status:      $statusPath"
    Write-Host "Command:     powershell -ExecutionPolicy Bypass -File $PSCommandPath -Action status"
    Write-Host ""
    exit 0
  }
}
