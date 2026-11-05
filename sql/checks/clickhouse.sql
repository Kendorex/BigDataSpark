-- ClickHouse: six tables, each containing a complete mart.
SHOW TABLES FROM analytics;

-- 1. Products: top 10 by units, revenue by category, ratings and reviews.
SELECT product_name, units, revenue, sales_rank FROM analytics.report_products
WHERE row_type = 'product' AND sales_rank <= 10 ORDER BY sales_rank;
SELECT product_category, revenue FROM analytics.report_products
WHERE row_type = 'category' ORDER BY revenue DESC;
SELECT product_name, avg_rating, review_count FROM analytics.report_products
WHERE row_type = 'product' ORDER BY product_name;

-- 2. Customers: top 10, country distribution, average check.
SELECT customer_email, customer_name, revenue, spending_rank FROM analytics.report_customers
WHERE row_type = 'customer' AND spending_rank <= 10 ORDER BY spending_rank;
SELECT customer_country, customer_count FROM analytics.report_customers
WHERE row_type = 'country' ORDER BY customer_count DESC, customer_country;
SELECT customer_email, sales_count, revenue, avg_check FROM analytics.report_customers
WHERE row_type = 'customer' ORDER BY revenue DESC, customer_email LIMIT 20;

-- 3. Time: monthly/yearly trends, period comparisons, monthly average check.
SELECT period, sales_count, units, revenue FROM analytics.report_time
WHERE row_type = 'month' ORDER BY period;
SELECT period, revenue, previous_revenue, revenue_change, revenue_change_pct
FROM analytics.report_time WHERE row_type = 'year' ORDER BY period;
SELECT period, revenue, previous_revenue, revenue_change, revenue_change_pct, avg_check
FROM analytics.report_time WHERE row_type = 'month' ORDER BY period;

-- 4. Stores: top 5 names/networks, location distribution, average check.
SELECT store_name, revenue, revenue_rank FROM analytics.report_stores
WHERE row_type = 'store' AND revenue_rank <= 5 ORDER BY revenue_rank;
SELECT store_country, store_city, sales_count, units, revenue FROM analytics.report_stores
WHERE row_type = 'location' ORDER BY revenue DESC LIMIT 20;
SELECT store_country, sum(sales_count) sales_count, sum(revenue) revenue
FROM analytics.report_stores WHERE row_type = 'location'
GROUP BY store_country ORDER BY revenue DESC;
SELECT store_name, avg_check FROM analytics.report_stores
WHERE row_type = 'store' ORDER BY revenue DESC LIMIT 20;

-- 5. Suppliers: top 5, mean listed product price, country distribution.
SELECT supplier_name, revenue, revenue_rank FROM analytics.report_suppliers
WHERE row_type = 'supplier' AND revenue_rank <= 5 ORDER BY revenue_rank;
SELECT supplier_name, avg_product_price FROM analytics.report_suppliers
WHERE row_type = 'supplier' ORDER BY revenue DESC LIMIT 20;
SELECT supplier_country, sales_count, units, revenue FROM analytics.report_suppliers
WHERE row_type = 'country' ORDER BY revenue DESC;

-- 6. Quality: highest/lowest ratings, correlation, review ranking.
SELECT product_name, avg_rating, best_rating_rank, worst_rating_rank
FROM analytics.report_quality ORDER BY best_rating_rank;
SELECT DISTINCT rating_sales_correlation FROM analytics.report_quality;
SELECT product_name, review_count, reviews_rank FROM analytics.report_quality ORDER BY reviews_rank;

-- IMPORTANT: choose ONE row_type for totals; subtotal rows overlap.
SELECT sum(sales_count) sales, sum(units) units, sum(revenue) revenue
FROM analytics.report_products WHERE row_type = 'product';
