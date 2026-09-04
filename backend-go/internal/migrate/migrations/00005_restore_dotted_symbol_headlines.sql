-- Older MCP normalization treated the period inside qualified symbols such as
-- AVGO.O as the end of the headline. Restore those generated headlines from
-- the already-preserved complete summary, then keep the event payload and its
-- indexed headline column consistent. The predicates intentionally target only
-- the known malformed "(SYMBOL." shape and are safe to run repeatedly.
UPDATE news_items
SET title = left(regexp_replace(summary, '\s+', ' ', 'g'), 120)
WHERE raw_metadata::jsonb->>'mcp_adapter' = 'jin10_flash_v1'
  AND title ~ '\([[:alnum:]]{1,12}\.$'
  AND summary LIKE title || '%'
  AND title <> left(regexp_replace(summary, '\s+', ' ', 'g'), 120);

UPDATE news_events AS event
SET headline = news.title,
    payload = jsonb_set(event.payload::jsonb, '{headline}', to_jsonb(news.title), false)
FROM news_items AS news
WHERE event.payload::jsonb->'news_item_ids' ? news.id::text
  AND event.headline ~ '\([[:alnum:]]{1,12}\.$'
  AND news.raw_metadata::jsonb->>'mcp_adapter' = 'jin10_flash_v1';
