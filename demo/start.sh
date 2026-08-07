#!/usr/bin/env bash
# Boots the QoD self-contained demo (uvx qod start --demo), then runs the
# LiteLLM proxy (via uvx) with the DuckLake sink enabled.
# Ctrl-C stops the proxy and tears QoD down with `uvx qod stop`.
#
# Prerequisites: uv installed, JDK 21 and duckdb on PATH (used by the QoD demo).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
RUN_DIR="$SCRIPT_DIR/.run"
mkdir -p "$RUN_DIR"

for port in 31338 20900 4000; do
  if nc -z localhost "$port" 2>/dev/null; then
    echo "Port $port is already in use - another QoD/proxy instance is running." >&2
    echo "Stop it first: $SCRIPT_DIR/stop.sh" >&2
    exit 1
  fi
done

echo "Starting the QoD demo (log: $RUN_DIR/qod.log)"
uvx qod start --demo > "$RUN_DIR/qod.log" 2>&1 &
QOD_PID=$!
trap 'echo; echo "Tearing down QoD"; uvx qod stop >/dev/null 2>&1 || true; kill "$QOD_PID" 2>/dev/null || true' EXIT

printf 'Waiting for the Flight SQL edge on :31338 '
edge_up=""
for _ in $(seq 1 90); do
  if nc -z localhost 31338 2>/dev/null; then edge_up=1; break; fi
  if ! kill -0 "$QOD_PID" 2>/dev/null; then
    echo; echo "QoD exited early; last log lines:" >&2
    tail -30 "$RUN_DIR/qod.log" >&2
    exit 1
  fi
  printf '.'; sleep 2
done
if [ -z "$edge_up" ]; then
  echo; echo "Flight SQL edge never came up; see $RUN_DIR/qod.log" >&2
  exit 1
fi
echo " up"

# Demo posture seeded by `qod start --demo`: tenant acme, pool bi, tenant-db
# acme_tpch (default schema tpch1), TLS on the edge with a self-signed cert.
export DUCKLAKE_SINK_ENDPOINT=grpc+tls://localhost:31338
export DUCKLAKE_SINK_TLS_SKIP_VERIFY=true
export DUCKLAKE_SINK_USERNAME=acme-admin
export DUCKLAKE_SINK_PASSWORD=demo-acme-admin
export DUCKLAKE_SINK_TENANT=acme
export DUCKLAKE_SINK_POOL=bi
export DUCKLAKE_SINK_SCHEMA_NAME=tpch1
export DUCKLAKE_SINK_BATCH_INTERVAL=5
export DUCKLAKE_SINK_CAPTURE_PAYLOADS=true

cat <<EOF

QoD is up. Connection info for DBeaver (Arrow Flight SQL driver):
  URL:       jdbc:arrow-flight-sql://localhost:31338/?tenant=acme&pool=bi&useEncryption=true&disableCertificateVerification=true
  User:      acme-admin
  Password:  demo-acme-admin
  Tables land in: tpch1.llm_requests and tpch1.llm_payloads

Starting the LiteLLM proxy on :4000 (master key sk-1234)...
EOF

# fastapi 0.140+ removed get_flat_dependant, which litellm's proxy still imports.
# --reinstall-package forces a rebuild of the sink from the working tree; uv
# otherwise reuses a cached build keyed on the unchanged version number.
uvx --from 'litellm[proxy]' --with 'fastapi<0.140' --with "$REPO_DIR" \
  --reinstall-package litellm-ducklake-sink \
  litellm --config "$SCRIPT_DIR/litellm-config.yaml" --port 4000
