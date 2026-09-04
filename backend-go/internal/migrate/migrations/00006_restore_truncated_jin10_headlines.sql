-- Jin10's title adapter can split decimal values and qualified symbols at an
-- ASCII period even though the complete text remains in summary. Repair only
-- high-confidence prefix truncations where the next character is adjacent.
UPDATE news_items
SET title = left(regexp_replace(summary, '\s+', ' ', 'g'), 120)
WHERE raw_metadata::jsonb->>'mcp_adapter' = 'jin10_flash_v1'
  AND right(title, 1) = '.'
  AND summary LIKE title || '%'
  AND substring(summary FROM char_length(title) + 1 FOR 1) <> ''
  AND substring(summary FROM char_length(title) + 1 FOR 1) !~ '^[[:space:]]$'
  AND title <> left(regexp_replace(summary, '\s+', ' ', 'g'), 120);

UPDATE news_events AS event
SET headline = news.title,
    payload = jsonb_set(event.payload::jsonb, '{headline}', to_jsonb(news.title), false)
FROM news_items AS news
WHERE event.payload::jsonb->'news_item_ids' ? news.id::text
  AND news.raw_metadata::jsonb->>'mcp_adapter' = 'jin10_flash_v1'
  AND right(event.headline, 1) = '.'
  AND news.summary LIKE event.headline || '%'
  AND substring(news.summary FROM char_length(event.headline) + 1 FOR 1) <> ''
  AND substring(news.summary FROM char_length(event.headline) + 1 FOR 1) !~ '^[[:space:]]$'
  AND event.headline <> news.title;
