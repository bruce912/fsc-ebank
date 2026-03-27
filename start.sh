#!/bin/bash
# FSC 電子支付資料系統啟動腳本

# 停止舊進程
pkill -f "web_app.py" 2>/dev/null
pkill -f "cloudflared" 2>/dev/null
sleep 1

# 啟動 Flask (port 8080 避開 macOS AirPlay port 5000)
cd "$(dirname "$0")"
PORT=8080 python3 web_app.py &
sleep 2

# 啟動 Cloudflare Tunnel
cloudflared tunnel --url http://127.0.0.1:8080 2>&1 | tee /tmp/cf_tunnel.log &

# 等待並顯示網址
sleep 8
URL=$(grep -oE "https://[a-z0-9-]+\.trycloudflare\.com" /tmp/cf_tunnel.log | head -1)
echo ""
echo "=============================="
echo "本機網址: http://127.0.0.1:8080"
echo "對外網址: $URL"
echo "=============================="
echo "按 Ctrl+C 停止所有服務"

# 保持前景等待
wait
