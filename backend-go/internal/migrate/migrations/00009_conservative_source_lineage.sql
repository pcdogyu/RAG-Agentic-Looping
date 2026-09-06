-- A publisher domain identifies where a story was observed, not automatically
-- the primary issuer of its factual claim. Correct the first 90-day P0
-- backfill conservatively: retain explicit first-party/primary and named wire
-- origins, but treat other self-reported domains as unresolved provenance.
UPDATE source_lineage l
SET original_publisher=NULL,
    syndication_group=NULL,
    parse_status='unknown',
    payload=(coalesce(l.payload::jsonb,'{}'::jsonb) || jsonb_build_object('lineage_correction','publisher_domain_not_proven_origin'))::json,
    updated_at=now()
FROM news_items n
WHERE l.news_item_id=n.id
  AND n.published_at >= now()-interval '90 days'
  AND l.parse_status='resolved'
  AND coalesce(l.payload::jsonb->>'original_source','') <> ''
  AND coalesce(l.payload::jsonb->>'original_source','')=coalesce(l.payload::jsonb->>'publisher_domain','')
  AND lower(coalesce(n.source_quality,'')) NOT IN ('official','primary')
  AND lower(coalesce(n.source,'')) NOT IN ('reuters','bloomberg','associated press','ap','xinhua','新华社');
