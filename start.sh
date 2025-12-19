#!/bin/bash
# AIREA API Startup Script
# Works for both local development and Render deployment

echo "🧠 Starting AIREA API..."

# In local dev, clear any zombie processes on port 8000
# (This won't affect Docker/Render since each container is fresh)
if [ -z "$RENDER" ]; then
    if command -v lsof &> /dev/null; then
        if lsof -ti :8000 > /dev/null 2>&1; then
            echo "⚠️  Clearing port 8000..."
            lsof -ti :8000 | xargs kill -9 2>/dev/null
            sleep 1
        fi
    fi
fi

# Start the API
echo "🚀 Launching AIREA API on port 8000..."
exec python3 airea_api_server_v2.py
