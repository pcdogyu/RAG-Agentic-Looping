#!/usr/bin/env bash
set -u
cd /opt/RAG-Agentic-Looping
TOK=$(grep '^ADMIN_API_TOKEN=' .env | cut -d= -f2- | tr -d '"\r\n')
curl -s http://127.0.0.1:8081/api/v1/admin/mcp-sources -H "X-Admin-Token: $TOK" | python3 -c '
import json, sys
d = [x for x in json.load(sys.stdin) if x["name"] == "newsliquid"][0]
print("status =", d["last_status"])
print("error  =", d["last_error"])
print("tools  =", [t["name"] for t in d["discovered_tools"]])
print("enabled=", d["enabled"])
'
