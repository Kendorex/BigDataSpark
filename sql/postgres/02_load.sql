-- COPY understands quoted newlines; counting physical lines would be incorrect.
SET datestyle = 'ISO, MDY';
DO $$
DECLARE
    file_path TEXT;
    column_list TEXT;
    i INTEGER;
BEGIN
    SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
    INTO column_list
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'mock_data'
      AND column_name NOT IN ('source_row_id', 'source_file');

    FOR i IN 0..9 LOOP
        file_path := '/data/MOCK_DATA' || CASE WHEN i = 0 THEN '' ELSE ' (' || i || ')' END || '.csv';
        EXECUTE format('COPY public.mock_data (%s) FROM %L WITH (FORMAT csv, HEADER true)', column_list, file_path);
        UPDATE public.mock_data SET source_file = file_path WHERE source_file IS NULL;
    END LOOP;
    IF (SELECT count(*) FROM public.mock_data) <> 10000 THEN
        RAISE EXCEPTION 'Expected exactly 10000 source rows';
    END IF;
END $$;
