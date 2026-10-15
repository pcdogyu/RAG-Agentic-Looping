#!/usr/bin/env bash
set -u
cd /opt/RAG-Agentic-Looping
DBURL=$(grep '^DATABASE_URL=' .env | cut -d= -f2- | tr -d '"\r\n')
DBUSER=$(echo "$DBURL" | sed -E 's#.*://([^:]+):.*#\1#')
DBNAME=$(echo "$DBURL" | sed -E 's#.*/([^/?]+).*#\1#')
echo "user=$DBUSER db=$DBNAME"
docker compose exec -T postgres psql -U "$DBUSER" -d "$DBNAME" -v ON_ERROR_STOP=0 <<'SQL'
docker compose exec -T postgres psql -U "$DBUSER" -d "$DBNAME" -v ON_ERROR_STOP=0 <<'SQL'
PREPARE p1(json, boolean, bytea, text, text) AS
UPDATE mcp_sources SET discovered_tools=CASE WHEN $2 OR $4 IS NOT NULL THEN CAST($3 AS json) ELSE discovered_tools END,last_status=$5,last_error=$4,last_checked_at=now(),updated_at=now() WHERE id='14d51f84-f387-45f4-914d-3d27842995c6';
EXECUTE p1('{"a":1}'::json, true, '[{"name":"search_news"}]', NULL, 'discovered');
SELECT last_status, left(discovered_tools::text, 60) FROM mcp_sources WHERE id='14d51f84-f387-45f4-914d-3d27842995c6';
DEALLOCATE p1;
SQL
docker logs market-loop-agent-go-api-1 --since 10m 2>&1 | grep -iE 'mcp|probe|persist' | tail -20
