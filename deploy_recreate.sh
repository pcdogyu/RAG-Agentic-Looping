set -x
cd /opt/RAG-Agentic-Looping
docker compose --profile go-shadow up -d --no-deps --force-recreate go-api
echo "API_UP_EXIT=$?"
docker compose --profile go-shadow up -d --no-deps --force-recreate go-research-worker
echo "WORKER_UP_EXIT=$?"
sleep 20
docker compose --profile go-shadow ps --format '{{.Name}}|{{.Service}}|{{.State}}|{{.Status}}'
