@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo ================================================
echo  FSC 電子支付資料系統 - Windows 打包腳本
echo ================================================
echo.

:: ── 檢查 Python ───────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo [錯誤] 找不到 Python，請先安裝 Python 3.11
    pause & exit /b 1
)

:: ── 安裝依賴 ──────────────────────────────────────
echo [1/4] 安裝 Python 依賴套件...
pip install Flask==2.3.3 Werkzeug==2.3.7 pywebview pyinstaller --quiet
if errorlevel 1 (
    echo [錯誤] 安裝套件失敗
    pause & exit /b 1
)
echo       完成

:: ── 清除舊的打包結果 ──────────────────────────────
echo [2/4] 清除舊的打包結果...
if exist dist\FSCTOOL rmdir /s /q dist\FSCTOOL
if exist build       rmdir /s /q build
echo       完成

:: ── PyInstaller 打包 ──────────────────────────────
echo [3/4] 執行 PyInstaller 打包（需要幾分鐘）...
pyinstaller fsctool.spec --noconfirm
if errorlevel 1 (
    echo [錯誤] PyInstaller 打包失敗，請查看上方錯誤訊息
    pause & exit /b 1
)
echo       完成

:: ── 複製資料庫 ────────────────────────────────────
echo [4/4] 複製資料庫到輸出目錄...
if exist fsc_ebank.db (
    copy /y fsc_ebank.db dist\FSCTOOL\fsc_ebank.db >nul
    echo       完成
) else (
    echo [警告] 找不到 fsc_ebank.db，請手動複製到 dist\FSCTOOL\
)

:: ── 複製圖示（若存在）────────────────────────────
if exist icon.ico (
    copy /y icon.ico dist\FSCTOOL\ >nul
)

echo.
echo ================================================
echo  打包完成！
echo  執行檔位置：dist\FSCTOOL\FSCTOOL.exe
echo  發布方式：  將 dist\FSCTOOL\ 整個資料夾壓縮發布
echo ================================================
echo.
pause
