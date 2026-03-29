param(
  [ValidateSet("start", "status", "stop", "foreground")]
  [string]$Action = "start",
  [string]$Keyword = "ai",
  [string]$StartDay = (Get-Date -Format "yyyy-MM-dd"),
  [string]$EndDay = (Get-Date -Format "yyyy-MM-dd"),
  [string]$OldestDay = "2009-06-26",
  [int]$CheckIntervalSeconds = 30,
  [int]$StallTimeoutSeconds = 900,
  [switch]$Headless,
  [switch]$DisableCdp
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root "MediaCrawler\.venv\Scripts\python.exe"
$monitorScript = Join-Path $root "scripts\crawl_monitor.py"
$statusPath = Join-Path $root "artifacts\ai_crawl_monitor\status.json"
$commandName = if ($Action -eq "foreground") { "run" } else { $Action }

if (-not (Test-Path $python)) {
  throw "MediaCrawler virtualenv python not found: $python"
}

if (-not (Test-Path $monitorScript)) {
  throw "Monitor script not found: $monitorScript"
}

$commonArgs = @(
  $monitorScript,
  $commandName,
  "--platform", "bili"
)

if ($Action -in @("start", "foreground")) {
  $commonArgs += @(
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
  if ($DisableCdp) {
    $commonArgs += "--disable-cdp"
  }
}

if ($Action -eq "status") {
  $commonArgs += "--json"
}

& $python @commonArgs
$exitCode = $LASTEXITCODE

if ($Action -eq "start" -and $exitCode -eq 0) {
  Write-Host ""
  Write-Host "Bilibili ai crawl monitor started."
  Write-Host "Status:  $statusPath"
  Write-Host "Command: powershell -ExecutionPolicy Bypass -File $PSCommandPath -Action status"
  Write-Host ""
}

exit $exitCode
