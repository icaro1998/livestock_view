[CmdletBinding()]
param(
  [string]$OutputBase = ".\output\flood",
  [string]$LogFile = ".\output\_index\autopilot_remaining_downloads.log",
  [string]$ReportJson = ".\output\_index\autopilot_report_latest.json",
  [string]$ReportMd = ".\output\_index\autopilot_report_latest.md",
  [int]$TargetTiles = 21
)

$ErrorActionPreference = "Stop"

$jobDefs = @(
  @{ Tag = "dw_2025H1"; Dataset = "dynamicworld"; Range = "2025-01-01 -> 2025-07-01" },
  @{ Tag = "dw_2025H2"; Dataset = "dynamicworld"; Range = "2025-07-01 -> 2026-01-01" },
  @{ Tag = "s3_2025H1"; Dataset = "s3olci"; Range = "2025-01-01 -> 2025-07-01" },
  @{ Tag = "s3_2025H2"; Dataset = "s3olci"; Range = "2025-07-01 -> 2026-01-01" },
  @{ Tag = "s2_2025H1"; Dataset = "sentinel2"; Range = "2025-01-01 -> 2025-07-01" },
  @{ Tag = "s2_2025H2"; Dataset = "sentinel2"; Range = "2025-07-01 -> 2026-01-01" }
)

function Get-WorkflowProcesses {
  @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "run_remaining_downloads_autopilot.ps1|run_additional_datasets_tiled.py|download_additional_datasets.py" })
}

function Get-JobStats {
  param(
    [string]$RunPath,
    [int]$Target
  )

  $manifestPath = Join-Path $RunPath "tile_manifest.csv"
  $rows = @()
  if (Test-Path $manifestPath) {
    $rows = @(Import-Csv $manifestPath)
  }

  $ok = [int](@($rows | Where-Object { $_.status -eq "ok" }).Count)
  $err = [int](@($rows | Where-Object { $_.status -eq "error" }).Count)
  $dry = [int](@($rows | Where-Object { $_.status -eq "dry_run" }).Count)
  $tot = [int]$rows.Count
  $pending = [math]::Max($Target - $ok, 0)

  $lastUtc = ""
  if ($rows.Count -gt 0) {
    $last = @($rows | Where-Object { $_.updated_utc } | Sort-Object updated_utc -Descending | Select-Object -First 1)
    if ($last.Count -gt 0) {
      $lastUtc = [string]$last[0].updated_utc
    }
  }

  $tilesDir = Join-Path $RunPath "tiles"
  $sizeBytes = 0
  $fileCount = 0
  $tifCount = 0
  if (Test-Path $tilesDir) {
    $files = @(Get-ChildItem $tilesDir -Recurse -File -ErrorAction SilentlyContinue)
    $fileCount = [int]$files.Count
    $sum = ($files | Measure-Object Length -Sum).Sum
    if ($sum) { $sizeBytes = [int64]$sum }
    $tifCount = [int](@($files | Where-Object { $_.Extension -eq ".tif" }).Count)
  }

  return [pscustomobject]@{
    RunPath = $RunPath
    ManifestPath = $manifestPath
    ManifestExists = (Test-Path $manifestPath)
    TotalTiles = $tot
    Ok = $ok
    Err = $err
    Dry = $dry
    Pending = $pending
    LastUpdateUtc = $lastUtc
    FileCount = $fileCount
    TifCount = $tifCount
    SizeMB = [math]::Round($sizeBytes / 1MB, 2)
    SizeGB = [math]::Round($sizeBytes / 1GB, 3)
  }
}

function Get-RecentRunLogLines {
  param(
    [string]$Path,
    [int]$Tail = 30
  )

  if (-not (Test-Path $Path)) { return @() }
  $all = @(Get-Content $Path -ErrorAction SilentlyContinue)
  if ($all.Count -eq 0) { return @() }

  $startIdx = 0
  for ($i = 0; $i -lt $all.Count; $i++) {
    if ($all[$i] -match "Autopilot start") { $startIdx = $i }
  }

  $slice = @($all[$startIdx..($all.Count - 1)])
  if ($slice.Count -le $Tail) { return $slice }
  return @($slice[($slice.Count - $Tail)..($slice.Count - 1)])
}

$rows = foreach ($j in $jobDefs) {
  $runPath = Join-Path $OutputBase ("additional_tiled_60km_{0}" -f $j.Tag)
  $stats = Get-JobStats -RunPath $runPath -Target $TargetTiles
  [pscustomobject]@{
    Job = $j.Tag
    Dataset = $j.Dataset
    Range = $j.Range
    Progress = "{0}/{1}" -f $stats.Ok, $TargetTiles
    TotalTiles = $stats.TotalTiles
    Ok = $stats.Ok
    Error = $stats.Err
    DryRun = $stats.Dry
    Pending = $stats.Pending
    LastUpdateUtc = if ($stats.LastUpdateUtc) { $stats.LastUpdateUtc } else { "" }
    SizeMB = $stats.SizeMB
    SizeGB = $stats.SizeGB
    FileCount = $stats.FileCount
    TifCount = $stats.TifCount
    RunPath = $stats.RunPath
    ManifestPath = $stats.ManifestPath
    ManifestExists = $stats.ManifestExists
  }
}

$completedJobs = [int](@($rows | Where-Object { $_.Ok -ge $TargetTiles }).Count)
$pendingJobs = [int](@($rows | Where-Object { $_.Ok -lt $TargetTiles }).Count)
$pendingTiles = [int](($rows | Measure-Object Pending -Sum).Sum)
$totalSizeMB = [math]::Round((($rows | Measure-Object SizeMB -Sum).Sum), 2)
$totalSizeGB = [math]::Round($totalSizeMB / 1024, 3)

$procs = Get-WorkflowProcesses
$active = @{
  total = [int](@($procs | Select-Object -ExpandProperty ProcessId -Unique).Count)
  autopilot = [int](@($procs | Where-Object { $_.CommandLine -match "run_remaining_downloads_autopilot.ps1" } | Select-Object -ExpandProperty ProcessId -Unique).Count)
  tiled = [int](@($procs | Where-Object { $_.CommandLine -match "run_additional_datasets_tiled.py" } | Select-Object -ExpandProperty ProcessId -Unique).Count)
  downloader = [int](@($procs | Where-Object { $_.CommandLine -match "download_additional_datasets.py" } | Select-Object -ExpandProperty ProcessId -Unique).Count)
}

$recentLog = Get-RecentRunLogLines -Path $LogFile -Tail 30

$report = [ordered]@{
  generated_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
  target_tiles_per_job = $TargetTiles
  totals = [ordered]@{
    completed_jobs = $completedJobs
    pending_jobs = $pendingJobs
    pending_tiles = $pendingTiles
    total_size_mb = $totalSizeMB
    total_size_gb = $totalSizeGB
  }
  active_processes = $active
  jobs = $rows
  log_file = $LogFile
  recent_log_lines = $recentLog
}

$jsonDir = Split-Path -Parent $ReportJson
if ($jsonDir -and -not (Test-Path $jsonDir)) {
  New-Item -ItemType Directory -Path $jsonDir -Force | Out-Null
}

$mdDir = Split-Path -Parent $ReportMd
if ($mdDir -and -not (Test-Path $mdDir)) {
  New-Item -ItemType Directory -Path $mdDir -Force | Out-Null
}

$report | ConvertTo-Json -Depth 8 | Set-Content -Path $ReportJson -Encoding utf8

$md = @()
$md += "# Autopilot Report"
$md += ""
$md += "- Generated UTC: $($report.generated_utc)"
$md += "- Target tiles/job: $TargetTiles"
$md += "- Completed jobs: $completedJobs"
$md += "- Pending jobs: $pendingJobs"
$md += "- Pending tiles: $pendingTiles"
$md += "- Total size: $totalSizeMB MB ($totalSizeGB GB)"
$md += ""
$md += "## Active Processes"
$md += ""
$md += "- Total: $($active.total)"
$md += "- Autopilot: $($active.autopilot)"
$md += "- Tiled: $($active.tiled)"
$md += "- Downloader: $($active.downloader)"
$md += ""
$md += "## Jobs"
$md += ""
$md += "| Job | Dataset | Range | Progress | Pending | Error | DryRun | SizeGB | RunPath |"
$md += "|---|---|---|---:|---:|---:|---:|---:|---|"
foreach ($r in $rows) {
  $md += "| $($r.Job) | $($r.Dataset) | $($r.Range) | $($r.Progress) | $($r.Pending) | $($r.Error) | $($r.DryRun) | $($r.SizeGB) | `$($r.RunPath)` |"
}
$md += ""
$md += "## Recent Log Lines"
$md += ""
$md += "```text"
$md += ($recentLog -join "`n")
$md += "```"
$md += ""

Set-Content -Path $ReportMd -Value ($md -join "`r`n") -Encoding utf8

Write-Host "Wrote report JSON: $ReportJson"
Write-Host "Wrote report MD:   $ReportMd"
Write-Host ("Summary: completed_jobs={0}, pending_jobs={1}, pending_tiles={2}, total_size_gb={3}" -f $completedJobs, $pendingJobs, $pendingTiles, $totalSizeGB)

