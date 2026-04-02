# -*- mode: python ; coding: utf-8 -*-
"""
FSC 電子支付資料系統 — PyInstaller 打包設定
打包指令（在 WEB/ 目錄下執行）：
    pyinstaller fsctool.spec
輸出：
    dist/FSCTOOL/FSCTOOL.exe   （執行檔）
    dist/FSCTOOL/fsc_ebank.db  （需手動複製或由此處加入 datas）
"""

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# ── 收集隱含模組 ──────────────────────────────────────────────
hidden_imports = (
    collect_submodules("flask")
    + collect_submodules("werkzeug")
    + collect_submodules("jinja2")
    + collect_submodules("webview")
    + [
        "pkg_resources.py2_compat",
        "clr",          # pywebview Windows 後端依賴（pythonnet）
    ]
)

# ── 需要打包進去的資料檔 ────────────────────────────────────────
datas = [
    ("templates",            "templates"),
    ("static",               "static"),
    ("資料庫欄位說明.md",    "."),
    # 資料庫體積大（1 GB），建議與 exe 同目錄發布，不打包進 exe
    # 若想一起打包可取消下行註解（打包時間和體積會大幅增加）：
    # ("fsc_ebank.db", "."),
]

# 加入 pywebview 的資料檔
datas += collect_data_files("webview")

a = Analysis(
    ["launcher.py"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["gunicorn", "tkinter._test"],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="FSCTOOL",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,          # False = 不顯示黑色命令視窗
    icon="icon.ico",        # 可替換為自己的 .ico 圖示（不存在時移除此行）
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="FSCTOOL",
)
