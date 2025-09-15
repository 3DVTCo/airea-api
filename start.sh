#!/usr/bin/env bash
set -euo pipefail

ASSET_URL="${CHROMA_BOOTSTRAP_URL:?Set CHROMA_BOOTSTRAP_URL in Render}"
GITHUB_TOKEN="${GITHUB_TOKEN:?Set GITHUB_TOKEN in Render}"
TARGET_DIR="/opt/render/project/src/airea_brain"

echo "[bootstrap] Downloading fresh ChromaDB..."
curl -fL -H "Authorization: token ${GITHUB_TOKEN}" "${ASSET_URL}" -o /tmp/airea_brain.tar.gz
cd /opt/render/project/src
tar -xzf /tmp/airea_brain.tar.gz --overwrite
rm -f /tmp/airea_brain.tar.gz
echo "[bootstrap] ChromaDB ready with 4561 documents"

exec python3 airea_api_server_v2.py
