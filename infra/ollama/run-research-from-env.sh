#!/bin/sh
set -eu

instance="${1:?research instance index is required}"
project_env="${OLLAMA_PROJECT_ENV_FILE:-/opt/RAG-Agentic-Looping/.env}"

read_project_value() {
  key="$1"
  fallback="$2"
  value=""
  if [ -f "$project_env" ]; then
    value=$(awk -v key="$key" '
      index($0, key "=") == 1 {
        sub("^[^=]*=", "")
        sub("\\r$", "")
        print
        exit
      }
    ' "$project_env")
  fi
  [ -n "$value" ] || value="$fallback"
  case "$value" in
    \"*\") value=${value#\"}; value=${value%\"} ;;
    \'*\') value=${value#\'}; value=${value%\'} ;;
  esac
  printf '%s' "$value"
}

case "$instance" in
  0)
    host="172.17.0.1:11435"
    cpus="0-9"
    node="0"
    ;;
  1)
    host="172.17.0.1:11436"
    cpus="20-29"
    node="1"
    ;;
  2)
    host="172.17.0.1:11439"
    cpus="30-39"
    node="1"
    ;;
  *)
    echo "unsupported research instance: $instance" >&2
    exit 2
    ;;
esac

export OLLAMA_HOST="$host"
export OLLAMA_KEEP_ALIVE="$(read_project_value OLLAMA_KEEP_ALIVE -1)"
export OLLAMA_MAX_LOADED_MODELS=1
export OLLAMA_NUM_PARALLEL=1
export OLLAMA_CONTEXT_LENGTH="$(read_project_value OLLAMA_RESEARCH_CONTEXT_LENGTH 16384)"
export OLLAMA_MAX_QUEUE="$(read_project_value OLLAMA_MAX_QUEUE 256)"
export OLLAMA_LOAD_TIMEOUT="$(read_project_value OLLAMA_LOAD_TIMEOUT 10m)"

exec numactl --physcpubind="$cpus" --membind="$node" /usr/local/bin/ollama serve
