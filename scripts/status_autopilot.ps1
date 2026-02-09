[CmdletBinding()]
param(
  [string]$OutputBase = ".\output\flood",
  [string]$LogFile = ".\output\_index\autopilot_remaining_downloads.log",
  [int]$TargetTiles = 21,
  [int]$RefreshSeconds = 15,
  [switch]$Once,
  [switch]$NoClear
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

function Get-ActiveJobTag {
  $procs = Get-WorkflowProcesses
  if (-not $procs) { return "" }

  # Prefer worker process if present (more precise active tile/job)
  $ordered = @(
    @($procs | Where-Object { $_.CommandLine -match "download_additional_datasets.py" })
    @($procs | Where-Object { $_.CommandLine -match "run_additional_datasets_tiled.py" })
    @($procs)
  )

  foreach ($group in $ordered) {
    foreach ($p in $group) {
      if ($p.CommandLine -match "additional_tiled_60km_([A-Za-z0-9_]+)") {
        return $matches[1]
      }
    }
  }

  return ""
}

function Get-JobStats {
  param(
    [string]$RunPath,
    [int]$Target
  )

  $manifestPath = Join-Path $RunPath "tile_manifest.csv"
  if (-not (Test-Path $manifestPath)) {
    return [pscustomobject]@{
      ManifestExists = $false
      ManifestPath = $manifestPath
      Total = 0
      Ok = 0
      Err = 0
      Dry = 0
      Pending = $Target
      LastUpdateUtc = ""
      EtaMinutes = $null
      MinPerTile = $null
      SizeMB = 0.0
      SizeGB = 0.0
    }
  }

  $rows = @(Import-Csv $manifestPath)
  $okRows = @($rows | Where-Object { $_.status -eq "ok" })
  $ok = [int]$okRows.Count
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

  $eta = $null
  $mpt = $null
  if ($okRows.Count -ge 2 -and $pending -gt 0) {
    $times = @()
    foreach ($row in $okRows) {
      $dt = [datetime]::MinValue
      if ([datetime]::TryParse([string]$row.updated_utc, [ref]$dt)) {
        $times += $dt
      }
    }
    $times = @($times | Sort-Object)
    if ($times.Count -ge 2) {
      $elapsedMin = ($times[-1] - $times[0]).TotalMinutes
      $intervalCount = [math]::Max($times.Count - 1, 1)
      if ($elapsedMin -gt 0) {
        $mpt = $elapsedMin / $intervalCount
        $eta = [math]::Ceiling($pending * $mpt)
      }
    }
  }

  $tilesDir = Join-Path $RunPath "tiles"
  $sizeBytes = 0
  if (Test-Path $tilesDir) {
    $sum = (Get-ChildItem $tilesDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum
    if ($sum) { $sizeBytes = [int64]$sum }
  }

  return [pscustomobject]@{
    ManifestExists = $true
    ManifestPath = $manifestPath
    Total = $tot
    Ok = $ok
    Err = $err
    Dry = $dry
    Pending = $pending
    LastUpdateUtc = $lastUtc
    EtaMinutes = $eta
    MinPerTile = $mpt
    SizeMB = [math]::Round($sizeBytes / 1MB, 2)
    SizeGB = [math]::Round($sizeBytes / 1GB, 3)
  }
}

function Get-RecentRunLogLines {
  param(
    [string]$Path,
    [int]$Tail = 12
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

while ($true) {
  if (-not $NoClear) { Clear-Host }

  $procs = Get-WorkflowProcesses
  $uniquePids = @($procs | Select-Object -ExpandProperty ProcessId -Unique)
  $autopCount = [int](@($procs | Where-Object { $_.CommandLine -match "run_remaining_downloads_autopilot.ps1" } | Select-Object -ExpandProperty ProcessId -Unique).Count)
  $tiledCount = [int](@($procs | Where-Object { $_.CommandLine -match "run_additional_datasets_tiled.py" } | Select-Object -ExpandProperty ProcessId -Unique).Count)
  $downCount = [int](@($procs | Where-Object { $_.CommandLine -match "download_additional_datasets.py" } | Select-Object -ExpandProperty ProcessId -Unique).Count)

  $activeTag = Get-ActiveJobTag
  if (-not $activeTag) { $activeTag = "-" }

  "{0} | Processes: {1}/4 (autopilot={2}, tiled={3}, downloader={4}) | ActiveJob={5}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $uniquePids.Count, $autopCount, $tiledCount, $downCount, $activeTag
  ""

  $table = foreach ($j in $jobDefs) {
    $runPath = Join-Path $OutputBase ("additional_tiled_60km_{0}" -f $j.Tag)
    $stats = Get-JobStats -RunPath $runPath -Target $TargetTiles
    $progress = "{0}/{1}" -f $stats.Ok, $TargetTiles
    $etaTxt = if ($stats.EtaMinutes -ne $null) {
      "~{0}m ({1} m/tile)" -f $stats.EtaMinutes, ([math]::Round($stats.MinPerTile, 2))
    } else {
      "-"
    }

    [pscustomobject]@{
      Job = $j.Tag
      Dataset = $j.Dataset
      Range = $j.Range
      Progress = $progress
      Pending = $stats.Pending
      Err = $stats.Err
      Dry = $stats.Dry
      SizeGB = $stats.SizeGB
      ETA = $etaTxt
      LastUpdateUtc = if ($stats.LastUpdateUtc) { $stats.LastUpdateUtc } else { "-" }
      RunPath = $runPath
    }
  }

  $table | Format-Table Job, Dataset, Range, Progress, Pending, Err, Dry, SizeGB, ETA, LastUpdateUtc -AutoSize
  ""
  "Run paths:"
  $table | Select-Object Job, RunPath | Format-Table -AutoSize
  ""
  "Recent log lines (current run):"
  $recent = Get-RecentRunLogLines -Path $LogFile -Tail 12
  if ($recent.Count -eq 0) {
    "(no log lines)"
  } else {
    $recent
  }

  if ($Once) { break }
  Start-Sleep -Seconds $RefreshSeconds
}
