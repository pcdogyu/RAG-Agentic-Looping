set -x
cd /opt/RAG-Agentic-Looping
docker compose --profile go-shadow build go-api go-research-worker
echo "BUILD_EXIT=$?"
