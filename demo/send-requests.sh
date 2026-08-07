#!/usr/bin/env bash
# Sends a few chat completions through the LiteLLM proxy started by start.sh.
# Usage: ./send-requests.sh [count]   (default 5)
set -euo pipefail

COUNT="${1:-5}"
PROXY_URL="${PROXY_URL:-http://localhost:4000}"

for i in $(seq 1 "$COUNT"); do
  curl -sS "$PROXY_URL/v1/chat/completions" \
    -H "Authorization: Bearer sk-1234" \
    -H 'Content-Type: application/json' \
    -d "{\"model\": \"mock-gpt\", \"messages\": [{\"role\": \"user\", \"content\": \"demo request $i\"}]}" \
    | python3 -c 'import sys, json; r = json.load(sys.stdin); print(r["id"], "->", r["choices"][0]["message"]["content"])'
done

echo
echo "Sent $COUNT requests. The sink flushes every 5 seconds; then query in DBeaver:"
echo "  SELECT request_id, deployment_model, prompt_tokens, completion_tokens, spend, status"
echo "  FROM tpch1.llm_requests ORDER BY request_ts DESC LIMIT 10"
