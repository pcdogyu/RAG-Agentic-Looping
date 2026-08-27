#!/bin/sh
set -eu

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

export OLLAMA_HOST="172.17.0.1:11437"
export OLLAMA_KEEP_ALIVE="$(read_project_value OLLAMA_KEEP_ALIVE -1)"
export OLLAMA_MAX_LOADED_MODELS=1
export OLLAMA_NUM_PARALLEL=1
export OLLAMA_CONTEXT_LENGTH="$(read_project_value OLLAMA_ASSIST_CONTEXT_LENGTH 16384)"
export OLLAMA_MAX_QUEUE="$(read_project_value OLLAMA_MAX_QUEUE 256)"
export OLLAMA_LOAD_TIMEOUT="$(read_project_value OLLAMA_LOAD_TIMEOUT 10m)"

exec numactl --physcpubind="8-15" --membind="0" /usr/local/bin/ollama serve
