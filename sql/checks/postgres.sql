DO $$
DECLARE
    raw_count BIGINT; fact_count BIGINT;
    raw_total NUMERIC; fact_total NUMERIC;
    raw_units BIGINT; fact_units BIGINT;
BEGIN
    SELECT count(*), sum(sale_total_price), sum(sale_quantity)
    INTO raw_count, raw_total, raw_units FROM mock_data;
    SELECT count(*), sum(sale_total_price), sum(sale_quantity)
    INTO fact_count, fact_total, fact_units FROM fact_sales;
    IF raw_count <> 10000 OR fact_count <> raw_count
       OR raw_total IS DISTINCT FROM fact_total
       OR raw_units IS DISTINCT FROM fact_units THEN
        RAISE EXCEPTION 'Validation failed: raw=(%,%,%) fact=(%,%,%)',
            raw_count, raw_total, raw_units, fact_count, fact_total, fact_units;
    END IF;
    -- PK + FK + NOT NULL constraints guarantee one valid dimension row per key.
    RAISE NOTICE 'OK: % sales, % units, revenue %', fact_count, fact_units, fact_total;
END $$;
