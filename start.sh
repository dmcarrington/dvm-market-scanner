#!/bin/bash
# DVM Market Scanner — Start API + ngrok tunnel
set -e

cd ~/.openclaw/workspace/dvm-market-scanner

# Kill existing processes
pkill -f "python3.*api.py" 2>/dev/null || true
pkill -f "ngrok http" 2>/dev/null || true
sleep 1

# Start API server
nohup python3 -u api.py >> api.log 2>&1 &
API_PID=$!
echo "API started (PID $API_PID)"

sleep 2

# Start ngrok tunnel
nohup ngrok http 8765 >> ngrok.log 2>&1 &
NGROK_PID=$!
echo "ngrok started (PID $NGROK_PID)"

sleep 5

# Get tunnel URL
TUNNEL_URL=$(curl -s http://localhost:4040/api/tunnels | python3 -c "import json,sys; print(json.load(sys.stdin)['tunnels'][0]['public_url'])")
echo "Tunnel URL: $TUNNEL_URL"

# Save tunnel URL for dashboard
echo "$TUNNEL_URL" > .tunnel_url

echo "Done. Dashboard: $TUNNEL_URL/dashboard"
