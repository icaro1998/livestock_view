@echo off
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" "scripts\run_yearly_snapshots.py"
) else (
  python "scripts\run_yearly_snapshots.py"
)

echo.
pause
