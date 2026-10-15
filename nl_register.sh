#!/usr/bin/env bash
set -u
cd /opt/RAG-Agentic-Looping
TOK=$(grep '^ADMIN_API_TOKEN=' .env | cut -d= -f2- | tr -d '"\r\n')
SID=14d51f84-f387-45f4-914d-3d27842995c6
BASE=http://127.0.0.1:8081/api/v1/admin/mcp-sources
echo '=== PUT update ==='
curl -s -X PUT "$BASE/$SID" -H 'Content-Type: application/json' -H "X-Admin-Token: $TOK" -d '{
  "name": "newsliquid",
  "url": "http://news-mcp:8080/mcp",
  "description": "NewsLiquid 6551.io 快速加密资产新闻源（内置 news-mcp 网关）",
  "priority": 100,
  "enabled": false,
  "auth_type": "bearer",
  "auth_header_name": null,
  "tool_mappings": {
    "news_search": {"tool_name": "search_news", "output_adapter": "news_items_v1"},
    "news_feed": {"tool_name": "get_latest_news", "output_adapter": "news_items_v1"}
  },
  "group_id": "crypto"
}'
echo
echo '=== DISCOVER ==='
curl -s -X POST "$BASE/$SID/discover" -H "X-Admin-Token: $TOK" | head -c 900
echo
echo '=== TEST ==='
curl -s -X POST "$BASE/$SID/test" -H "X-Admin-Token: $TOK" | head -c 500
