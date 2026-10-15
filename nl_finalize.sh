#!/usr/bin/env bash
set -u
cd /opt/RAG-Agentic-Looping
TOK=$(grep '^ADMIN_API_TOKEN=' .env | cut -d= -f2- | tr -d '"\r\n')
SID=14d51f84-f387-45f4-914d-3d27842995c6
BASE=http://127.0.0.1:8081/api/v1/admin
ST() { python3 -c 'import json,sys; d=json.load(sys.stdin); s=d["source"] if "source" in d else d; print("status:", s["last_status"], "| error:", s["last_error"], "| tools:", [t["name"] for t in s["discovered_tools"]], "| enabled:", s["enabled"])'; }
echo '=== PUT mappings with input_bindings ==='
curl -s -X PUT "$BASE/mcp-sources/$SID" -H 'Content-Type: application/json' -H "X-Admin-Token: $TOK" -d '{
  "name": "newsliquid",
  "url": "http://news-mcp:8080/mcp",
  "description": "NewsLiquid 6551.io 快速加密资产新闻源（内置 news-mcp 网关）",
  "priority": 100,
  "enabled": true,
  "auth_type": "bearer",
  "auth_header_name": null,
  "tool_mappings": {
    "news_search": {"tool_name": "search_news", "output_adapter": "news_items_v1", "input_bindings": {"query": "keyword", "limit": "limit"}},
    "news_feed": {"tool_name": "get_latest_news", "output_adapter": "news_items_v1", "input_bindings": {"limit": "limit"}}
  },
  "group_id": "crypto"
}' | python3 -c 'import json,sys; d=json.load(sys.stdin); print("mappings:", json.dumps(d["tool_mappings"]))'
echo '=== DISCOVER ==='
curl -s -X POST "$BASE/mcp-sources/$SID/discover" -H "X-Admin-Token: $TOK" | ST
echo '=== TEST ==='
curl -s -X POST "$BASE/mcp-sources/$SID/test" -H "X-Admin-Token: $TOK" | ST
echo '=== ENABLE ==='
curl -s -X PATCH "$BASE/mcp-sources/$SID/enabled" -H 'Content-Type: application/json' -H "X-Admin-Token: $TOK" -d '{"enabled":true}' | python3 -c 'import json,sys; d=json.load(sys.stdin); print("enabled:", d.get("enabled"), "status:", d.get("last_status"), "err:", d.get("last_error"))'
echo '=== SEARCH (newsliquid only) ==='
curl -s -X POST "$BASE/search" -H 'Content-Type: application/json' -H "X-Admin-Token: $TOK" -d "{\"query\":\"bitcoin\",\"source_id\":\"$SID\",\"language\":\"all\",\"time_range\":\"day\",\"limit\":5}" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print("items:", len(d.get("items", [])), "errors:", d.get("errors"))
for it in d.get("items", [])[:3]:
    print("-", it.get("title", "")[:80], "|", it.get("url", "")[:60], "|", it.get("published_at"))
'
