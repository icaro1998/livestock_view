param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsForPython
)

$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$python = if (Test-Path ".\.venv\Scripts\python.exe") {
    ".\.venv\Scripts\python.exe"
} else {
    "python"
}

$mutexName = "Global\livestock_view_run_yearly_snapshots"
$mutex = New-Object System.Threading.Mutex($false, $mutexName)
$hasLock = $mutex.WaitOne(0, $false)
if (-not $hasLock) {
    throw "Another run_yearly_snapshots workflow is already running. Stop it first."
}

try {
    Write-Host "Using Python: $python"
    Write-Host "==> Running yearly snapshots..."
    & $python ".\scripts\run_yearly_snapshots.py" @ArgsForPython
    if ($LASTEXITCODE -ne 0) {
        throw "run_yearly_snapshots.py failed with exit code $LASTEXITCODE"
    }

    Write-Host "==> Rebuild index + validate outputs..."
    & ".\scripts\rebuild_and_validate.ps1"
    if ($LASTEXITCODE -ne 0) {
        throw "rebuild_and_validate.ps1 failed with exit code $LASTEXITCODE"
    }

    Write-Host "End-to-end workflow completed."
}
finally {
    if ($hasLock) {
        $mutex.ReleaseMutex() | Out-Null
    }
    $mutex.Dispose()
}
