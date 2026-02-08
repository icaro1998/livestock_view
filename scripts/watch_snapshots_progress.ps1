param(
    [int]$IntervalSeconds = 2,
    [int]$TailLines = 10
)

$ErrorActionPreference = "SilentlyContinue"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$prevLogSize = -1
$staleSeconds = 0

while ($true) {
    $targets = Get-CimInstance Win32_Process |
        Where-Object { $_.CommandLine -match "scripts\\run_yearly_snapshots.py|scripts\\flood_pipeline.py" }

    $proc = $targets |
        Where-Object { $_.CommandLine -match "scripts\\flood_pipeline.py --mode snapshots" } |
        Select-Object -First 1

    $running = [bool]$proc
    $chunkRange = "N/A"
    $logPath = $null

    if ($running) {
        $cmd = $proc.CommandLine
        if ($cmd -match "--s1-series-start ([0-9-]+).*--s1-series-end ([0-9-]+)") {
            $chunkRange = "$($matches[1]) -> $($matches[2])"
        }
        if ($cmd -match "--log-file ([^ ]+)") {
            $rawLog = $matches[1]
            $logPath = if ([System.IO.Path]::IsPathRooted($rawLog)) {
                $rawLog
            } else {
                Join-Path $repoRoot $rawLog
            }
        }
    }

    if (-not $logPath) {
        $latest = Get-ChildItem ".\output\flood\logs\run_range_*.log" |
            Sort-Object Name -Descending |
            Select-Object -First 1
        if ($latest) {
            $logPath = $latest.FullName
        }
    }

    $snapshotFiles = Get-ChildItem ".\output\flood\snapshots\s1_flood_diff_*.tif" |
        Sort-Object LastWriteTime -Descending
    $snapCount = @($snapshotFiles).Count
    $lastSnapshot = if ($snapCount -gt 0) { $snapshotFiles[0].Name } else { "(none)" }
    $lastSnapshotTime = if ($snapCount -gt 0) { $snapshotFiles[0].LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss") } else { "-" }

    $logSizeMb = 0.0
    $lastLogTime = "-"
    $totalRamGb = 0.0
    $availRamGb = 0.0
    $usedRamGb = 0.0
    $procWsGb = 0.0

    $os = Get-CimInstance Win32_OperatingSystem
    if ($os) {
        $totalRamGb = [math]::Round(([double]$os.TotalVisibleMemorySize * 1KB) / 1GB, 2)
        $availRamGb = [math]::Round(([double]$os.FreePhysicalMemory * 1KB) / 1GB, 2)
        $usedRamGb = [math]::Round($totalRamGb - $availRamGb, 2)
    }

    if ($targets) {
        $procWsBytes = 0.0
        foreach ($t in $targets) {
            $procWsBytes += [double]$t.WorkingSetSize
        }
        $procWsGb = [math]::Round($procWsBytes / 1GB, 2)
    }

    if ($logPath -and (Test-Path $logPath)) {
        $logItem = Get-Item $logPath
        $logSizeMb = [math]::Round($logItem.Length / 1MB, 2)
        $lastLogTime = $logItem.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
        if ($prevLogSize -ge 0 -and $logItem.Length -eq $prevLogSize) {
            $staleSeconds += $IntervalSeconds
        } else {
            $staleSeconds = 0
        }
        $prevLogSize = $logItem.Length
    } else {
        $staleSeconds = 0
        $prevLogSize = -1
    }

    Clear-Host
    Write-Host "TIME:            $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    Write-Host "RUNNING:         $running"
    Write-Host "PY PROCS:        $(@($targets).Count)"
    Write-Host "RAM TOTAL (GB):  $totalRamGb"
    Write-Host "RAM USED  (GB):  $usedRamGb"
    Write-Host "RAM AVAIL (GB):  $availRamGb"
    Write-Host "PY WS   (GB):    $procWsGb"
    Write-Host "CURRENT RANGE:   $chunkRange"
    Write-Host "SNAPSHOTS:       $snapCount"
    Write-Host "LAST SNAPSHOT:   $lastSnapshot"
    Write-Host "LAST SNAP TIME:  $lastSnapshotTime"
    Write-Host "LOG:             $logPath"
    Write-Host "LOG SIZE (MB):   $logSizeMb"
    Write-Host "LOG LAST WRITE:  $lastLogTime"
    Write-Host "STALE (seconds): $staleSeconds"
    if ($staleSeconds -ge 600 -and $running) {
        Write-Host "WARNING: no log growth for >= 10 minutes." -ForegroundColor Yellow
    }
    Write-Host ""
    if ($logPath -and (Test-Path $logPath)) {
        Write-Host ("LAST {0} LOG LINES:" -f $TailLines)
        Get-Content $logPath -Tail $TailLines
    } else {
        Write-Host "No run log found yet."
    }

    Start-Sleep -Seconds $IntervalSeconds
}
