$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$python = if (Test-Path ".\.venv\Scripts\python.exe") {
    ".\.venv\Scripts\python.exe"
} else {
    "python"
}

Write-Host "Using Python: $python"

Write-Host "==> Rebuilding dataset index..."
& $python ".\scripts\rebuild_dataset_index.py"
if ($LASTEXITCODE -ne 0) {
    throw "rebuild_dataset_index.py failed with exit code $LASTEXITCODE"
}

$logPath = ".\output\_index\validate_rasters.log"
Write-Host "==> Validating outputs..."
& $python ".\scripts\validate_rasters.py" --root ".\output" | Tee-Object $logPath
if ($LASTEXITCODE -ne 0) {
    throw "validate_rasters.py failed with exit code $LASTEXITCODE (see $logPath)"
}

Write-Host "Workflow completed."
Write-Host "Validation log: $logPath"
