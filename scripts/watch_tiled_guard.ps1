param(
  [string]$RunRoot = ".\output\flood\additional_tiled_60km_2025",
  [int]$TargetTiles = 21,
  [int]$PollSeconds = 15,
  [int]$StallMinutes = 12,
  [string]$ProcessPattern = "run_additional_datasets_tiled.py|download_additional_datasets.py"
)

$manifest = Join-Path $RunRoot "tile_manifest.csv"
$stallLimitSec = $StallMinutes * 60
# Normalize trailing separators robustly (works across PS versions)
$normalizedRunRoot = ($RunRoot -replace '[\\/]+$','')
$runTag = Split-Path $normalizedRunRoot -Leaf

function Invoke-Alarm([string]$msg) {
  Write-Host ""
  Write-Host "ALERT: $msg" -ForegroundColor Red
  1..12 | ForEach-Object {
    try { [console]::Beep(1400,250) } catch { [System.Media.SystemSounds]::Hand.Play() }
    Start-Sleep -Milliseconds 120
  }
}

function Invoke-Success([string]$msg) {
  Write-Host ""
  Write-Host $msg -ForegroundColor Green
  1..3 | ForEach-Object {
    try { [console]::Beep(1100,180) } catch { [System.Media.SystemSounds]::Asterisk.Play() }
    Start-Sleep -Milliseconds 120
  }
}

Write-Host "Guard monitor started | Poll=${PollSeconds}s | Stall=${StallMinutes}m"
Write-Host "Manifest: $manifest"
Write-Host "ProcessPattern: $ProcessPattern"
Write-Host "RunTag: $runTag"
Write-Host "Ctrl+C to stop."
Write-Host ""

while ($true) {
  $now = Get-Date

  # Only count processes tied to this run root/tag.
  $procs = Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -match $ProcessPattern -and $_.CommandLine -match [regex]::Escape($runTag)
  }
  $running = [bool]$procs
  $procCount = @($procs).Count

  if (Test-Path $manifest) {
    $rows = @(Import-Csv $manifest)
    $total = [int]$rows.Count
    if ($TargetTiles -le 0 -and $total -gt 0) { $TargetTiles = $total }

    $ok  = [int](@($rows | Where-Object { $_.status -eq "ok" }).Count)
    $err = [int](@($rows | Where-Object { $_.status -eq "error" }).Count)
    $dry = [int](@($rows | Where-Object { $_.status -eq "dry_run" }).Count)

    # Treat only "ok" as completed work; "error" must be retried.
    $done = $ok
    $pending = [math]::Max($TargetTiles - $done, 0)

    # Activity timestamp = max(manifest update, latest output file write)
    $latestManifestUtc = $null
    $latest = $rows | Sort-Object updated_utc -Descending | Select-Object -First 1
    if ($latest -and $latest.updated_utc) {
      try { $latestManifestUtc = ([datetime]$latest.updated_utc).ToUniversalTime() } catch {}
    }

    $latestFileUtc = $null
    $latestFile = Get-ChildItem $RunRoot -Recurse -File -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTime -Descending |
      Select-Object -First 1
    if ($latestFile) {
      $latestFileUtc = $latestFile.LastWriteTime.ToUniversalTime()
    }

    $activityUtc = $latestManifestUtc
    if ($latestFileUtc -and (-not $activityUtc -or $latestFileUtc -gt $activityUtc)) {
      $activityUtc = $latestFileUtc
    }

    $staleSec = 999999
    if ($activityUtc) {
      $staleSec = [int](($now.ToUniversalTime() - $activityUtc).TotalSeconds)
    }

    "{0} | running={1} proc={2} | total={3} ok={4} err={5} dry={6} pending={7} | stale={8}s" -f `
      ($now.ToString("HH:mm:ss")), $running, $procCount, $total, $ok, $err, $dry, $pending, $staleSec

    if ($ok -ge $TargetTiles -and -not $running) {
      Invoke-Success "DONE: tiles completed ($ok/$TargetTiles)."
      break
    }

    if (-not $running -and $pending -gt 0) {
      Invoke-Alarm "Process stopped with pending tiles ($pending)."
      break
    }

    if ($running -and $staleSec -ge $stallLimitSec) {
      Invoke-Alarm "No manifest/file activity for $staleSec seconds while process is running."
      break
    }
  }
  else {
    "{0} | manifest=missing | running={1} proc={2}" -f ($now.ToString("HH:mm:ss")), $running, $procCount
    if (-not $running) {
      Invoke-Alarm "No process running and manifest not found."
      break
    }
  }

  Start-Sleep -Seconds $PollSeconds
}
