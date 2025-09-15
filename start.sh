#!/usr/bin/env bash
set -euo pipefail

ASSET_URL="${CHROMA_BOOTSTRAP_URL:?Set CHROMA_BOOTSTRAP_URL in Render}"
GITHUB_TOKEN="${GITHUB_TOKEN:?Set GITHUB_TOKEN in Render}"
TARGET_DIR="/opt/render/project/src/airea_brain"

if [ ! -f "${TARGET_DIR}/chroma.sqlite3" ]; then
  echo "[bootstrap] fetching ChromaDB..."
  rm -rf "${TARGET_DIR}"
  curl -fL -H "Authorization: token ${GITHUB_TOKEN}" "${ASSET_URL}" -o airea_brain.tar.gz
  tar -xzf airea_brain.tar.gz
  rm -f airea_brain.tar.gz
  echo "[bootstrap] ChromaDB ready"
fi

exec python3 airea_api_server_v2.py
