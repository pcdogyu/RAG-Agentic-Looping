set -e
cd /opt/RAG-Agentic-Looping
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -F"|"' <<'SQL'
\echo == runs/impacts totals ==
SELECT count(*) AS runs,
       count(*) FILTER (WHERE jsonb_typeof(payload::jsonb->'report'->'impacts') = 'array') AS runs_with_impacts
FROM event_research_runs;
\echo == target_type distribution ==
SELECT COALESCE(i->>'target_type','(null)') AS target_type, count(*) AS n
FROM event_research_runs er
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(er.payload::jsonb->'report'->'impacts','[]'::jsonb)) i
GROUP BY 1 ORDER BY 2 DESC LIMIT 40;
\echo == distinct bare targets (not asset-backed) ==
SELECT COALESCE(i->>'target_type','(null)') AS target_type,
       COALESCE(i->>'target_name','(null)') AS target_name,
       count(*) AS n
FROM event_research_runs er
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(er.payload::jsonb->'report'->'impacts','[]'::jsonb)) i
WHERE COALESCE(i->>'asset_id','') = '' AND COALESCE(i->>'instrument_id','') = ''
GROUP BY 1,2 ORDER BY 3 DESC, 1, 2 LIMIT 160;
SQL
