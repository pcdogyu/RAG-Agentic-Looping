#!/bin/sh
set -eu

backend="${1:-}"
case "$backend" in
  go)
    upstream="http://go-api:8081"
    ;;
  python)
    upstream="http://api:8000"
    ;;
  *)
    echo "usage: $0 {go|python}" >&2
    exit 2
    ;;
esac

WEB_API_UPSTREAM="$upstream" \
WEB_API_BACKEND_NAME="$backend" \
  docker compose up -d --force-recreate --no-deps web

health_url="${WEB_HEALTH_URL:-http://127.0.0.1/health}"
headers_file="$(mktemp)"
trap 'rm -f "$headers_file"' EXIT HUP INT TERM

attempt=1
while [ "$attempt" -le 30 ]; do
  if curl -fsS -D "$headers_file" -o /dev/null "$health_url" && \
    grep -Eiq "^X-API-Backend:[[:space:]]*$backend\r?$" "$headers_file"; then
    echo "web API backend switched to $backend; $health_url returned success"
    exit 0
  fi
  attempt=$((attempt + 1))
  sleep 1
done

echo "web API backend switch to $backend failed health verification" >&2
exit 1
