#!/usr/bin/env bash
# Tears down everything start.sh launched: the LiteLLM proxy and the QoD demo
# (uvx qod stop also deletes the ephemeral demo data). Safe to run twice.
set -uo pipefail

pkill -f "litellm --config" 2>/dev/null && echo "Stopped the LiteLLM proxy" || echo "LiteLLM proxy not running"
sleep 2

uvx qod stop >/dev/null 2>&1 && echo "Stopped QoD" || echo "QoD not running"

for _ in $(seq 1 15); do
  pgrep -f "qod start|quack-on-demand-assembly|litellm --config" >/dev/null || break
  sleep 2
done

leftover=$(pgrep -fl "qod start|quack-on-demand-assembly|litellm --config" || true)
if [ -n "$leftover" ]; then
  echo "Still running, kill manually:" >&2
  echo "$leftover" >&2
  exit 1
fi

for port in 4000 20900 31338; do
  if nc -z localhost "$port" 2>/dev/null; then
    echo "Port $port is still in use by another process" >&2
    exit 1
  fi
done
echo "All demo processes stopped, ports 4000/20900/31338 free"
