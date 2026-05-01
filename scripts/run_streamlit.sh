#!/usr/bin/env bash
set -euo pipefail

cd /data_nuevo/cobertura_integrada

export JAVA_TOOL_OPTIONS="-Doracle.jdbc.timezoneAsRegion=false"

if [ -x "venv/bin/python" ]; then
  PYTHON="venv/bin/python"
else
  PYTHON="/usr/bin/python3"
fi

exec "$PYTHON" -m streamlit run app.py \
  --server.address 0.0.0.0 \
  --server.port 8501 \
  --server.headless true
