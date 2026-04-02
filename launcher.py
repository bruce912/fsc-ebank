#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FSC 電子支付資料系統 — 桌面啟動器
使用 PyInstaller 打包後的主入口點
"""

import sys
import threading
import socket
import time
import webview
from web_app import app


def find_free_port(start: int = 8080) -> int:
    """從 start 開始尋找可用的 port"""
    for port in range(start, start + 100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return port
    raise RuntimeError("找不到可用的 port")


def wait_for_server(port: int, timeout: int = 15) -> bool:
    """等待 Flask 伺服器就緒"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    return True
        except OSError:
            pass
        time.sleep(0.2)
    return False


def run_flask(port: int) -> None:
    """在背景執行緒中啟動 Flask"""
    app.run(host="127.0.0.1", port=port, debug=False, use_reloader=False)


def main() -> None:
    port = find_free_port(8080)

    flask_thread = threading.Thread(target=run_flask, args=(port,), daemon=True)
    flask_thread.start()

    if not wait_for_server(port):
        import tkinter.messagebox as mb
        mb.showerror("啟動失敗", "Flask 伺服器無法啟動，請重試。")
        sys.exit(1)

    webview.create_window(
        title="FSC 電子支付資料系統",
        url=f"http://127.0.0.1:{port}",
        width=1400,
        height=900,
        min_size=(1024, 600),
        resizable=True,
    )
    webview.start()


if __name__ == "__main__":
    main()
