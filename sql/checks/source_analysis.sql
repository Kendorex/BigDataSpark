-- PostgreSQL: run in DBeaver or psql before/after the ETL.
SELECT source_file, count(*) FROM mock_data GROUP BY source_file ORDER BY source_file;
SELECT count(*) rows, count(DISTINCT id) distinct_source_ids,
       sum(sale_quantity) units, sum(sale_total_price) revenue FROM mock_data;
SELECT id, count(*) FROM mock_data GROUP BY id HAVING count(*) > 1 ORDER BY id LIMIT 10;
SELECT count(*) price_mismatches FROM mock_data
WHERE product_price * sale_quantity <> sale_total_price;
SELECT product_name, count(*) FROM mock_data GROUP BY product_name;
SELECT min(sale_date), max(sale_date), count(DISTINCT customer_email) customers FROM mock_data;
SELECT 'raw' entity, count(*) FROM mock_data
UNION ALL SELECT 'facts', count(*) FROM fact_sales
UNION ALL SELECT 'customers', count(*) FROM dim_customer
UNION ALL SELECT 'sellers', count(*) FROM dim_seller
UNION ALL SELECT 'products', count(*) FROM dim_product
UNION ALL SELECT 'stores', count(*) FROM dim_store
UNION ALL SELECT 'suppliers', count(*) FROM dim_supplier
UNION ALL SELECT 'dates', count(*) FROM dim_date;
