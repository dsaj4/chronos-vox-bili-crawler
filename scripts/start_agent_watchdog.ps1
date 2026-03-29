param(
  [ValidateSet("once", "loop")]
  [string]$Action = "once",
  [ValidateSet("all", "bili", "zhihu", "xhs")]
  [string]$Platform = "all",
  [int]$IntervalSeconds = 900,
  [switch]$DryRunRepair,
  [switch]$NoAutoAdvance
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $RepoRoot "MediaCrawler\.venv\Scripts\python.exe"
$Script = Join-Path $RepoRoot "scripts\agent_watchdog.py"

if (-not (Test-Path $Python)) {
  throw "Python not found: $Python"
}
if (-not (Test-Path $Script)) {
  throw "Script not found: $Script"
}

$Args = @($Script)
if ($Action -eq "loop") {
  $Args += "run-loop"
  $Args += "--interval-seconds"
  $Args += "$IntervalSeconds"
} else {
  $Args += "run-once"
}
$Args += "--platform"
$Args += "$Platform"
$Args += "--json"
if ($DryRunRepair) {
  $Args += "--dry-run-repair"
}
if ($NoAutoAdvance) {
  $Args += "--no-auto-advance"
}

& $Python @Args
exit $LASTEXITCODE
