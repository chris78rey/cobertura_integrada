#!/usr/bin/env bash
set -euo pipefail

cd /data_nuevo/cobertura_integrada

export JAVA_TOOL_OPTIONS="-Doracle.jdbc.timezoneAsRegion=false"

if [ -x "venv/bin/python" ]; then
  PYTHON="venv/bin/python"
else
  PYTHON="/usr/bin/python3"
fi

exec "$PYTHON" scripts/resume_coberturas_auto.py
