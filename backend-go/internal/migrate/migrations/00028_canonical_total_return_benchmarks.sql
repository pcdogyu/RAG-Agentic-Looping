-- Canonical market-policy benchmarks are deliberately inactive: they provide
-- point-in-time benchmark observations without entering the ordinary research
-- universe. Exact codes and currencies come from the index administrators.
INSERT INTO assets(
    id,asset_class,market,symbol,name,exchange_or_provider,currency,
    aliases,products,competitors,lot_size,active,instrument_type,
    association_tier,association_reason,provider_association_tier,provider_association_reason
) VALUES
    (
        'index:CSI:H00300','index','CN','H00300','沪深300全收益指数','CSI','CNY',
        '[".CSIH00300","CSIR0300","CSI 300 Total Return Index"]'::json,'[]'::json,'[]'::json,
        1,false,'gross_total_return_index','strict','official_index_identity','strict','official_index_identity'
    ),
    (
        'index:HSI:HSIDV','index','HK','HSIDV','恒生指数总回报指数','Hang Seng Indexes','HKD',
        '[".HSIDV","HSIRH","HSIRH.HI","Hang Seng Index Gross Total Return Index"]'::json,'[]'::json,'[]'::json,
        1,false,'gross_total_return_index','strict','official_index_identity','strict','official_index_identity'
    )
ON CONFLICT (id) DO NOTHING;
