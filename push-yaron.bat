@echo off
REM ========================================
REM Yaron Monday - Smart Push
REM ========================================

echo.
echo ========================================
echo    Yaron Monday - Smart Push
echo ========================================
echo.

cd /d "%~dp0"

REM Use PowerShell to extract commit message from index-yaron.html
echo [*] Reading commit message from index-yaron.html...

for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "(Get-Content 'index-yaron.html' | Select-Object -Skip 1 -First 1) -replace '.*COMMIT:\s*(.+?)\s*--.*','$1'"`) do set "message=%%i"

REM Check if we got a valid message
echo %message% | findstr /C:"<!--" >nul
if %errorlevel% equ 0 (
    set "message=Update Yaron tasks dashboard"
    echo [!] No commit message found
    echo [*] Using default: %message%
) else if "%message%"=="" (
    set "message=Update Yaron tasks dashboard"
    echo [!] No commit message found
    echo [*] Using default: %message%
) else (
    echo [+] Found message: %message%
)

echo.
echo [1/3] Adding files...
git add index-yaron.html

echo.
echo [2/3] Committing...
git commit -m "%message%"

if errorlevel 1 (
    echo.
    echo [i] No changes to commit
    pause
    exit /b 0
)

echo.
echo [3/3] Pushing to GitHub...
git push

if errorlevel 1 (
    echo.
    echo [!] Push failed
    pause
    exit /b 1
)

echo.
echo ========================================
echo    Done! Yaron dashboard deployed!
echo ========================================
echo.
pause
