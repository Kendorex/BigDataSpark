from decimal import Decimal
from pathlib import Path

from common import ch_query, pg_connection, pg_execute, publish_report, read_pg


REPORT_SQL = {
    "report_products": """
        SELECT 'product' row_type, product_name, '' product_category,
               sales_count, units, revenue, avg_rating, review_count,
               row_number() OVER (ORDER BY units DESC, revenue DESC, product_name) sales_rank
        FROM product_metrics
        UNION ALL
        SELECT 'category', '', product_category, count(*), sum(sale_quantity),
               sum(sale_total_price), cast(NULL AS DOUBLE), cast(NULL AS BIGINT), cast(NULL AS INT)
        FROM sales GROUP BY product_category
    """,
    "report_customers": """
        WITH customers AS (
            SELECT customer_email, max(concat_ws(' ', customer_first_name, customer_last_name)) customer_name,
                   count(*) sales_count, sum(sale_quantity) units, sum(sale_total_price) revenue,
                   avg(sale_total_price) avg_check
            FROM sales GROUP BY customer_email
        )
        SELECT 'customer' row_type, customer_email, customer_name, '' customer_country,
               cast(1 AS BIGINT) customer_count, sales_count, units, revenue, avg_check,
               row_number() OVER (ORDER BY revenue DESC, customer_email) spending_rank
        FROM customers
        UNION ALL
        SELECT 'country', '', '', customer_country, count(DISTINCT customer_email),
               count(*), sum(sale_quantity), sum(sale_total_price), avg(sale_total_price), cast(NULL AS INT)
        FROM sales GROUP BY customer_country
    """,
    "report_time": """
        WITH bounds AS (
            SELECT trunc(min(full_date), 'month') lo, trunc(max(full_date), 'month') hi FROM sales
        ), calendar AS (
            SELECT explode(sequence(lo, hi, interval 1 month)) month_start FROM bounds
        ), monthly AS (
            SELECT trunc(full_date, 'month') month_start, count(*) sales_count,
                   sum(sale_quantity) units, sum(sale_total_price) revenue
            FROM sales GROUP BY trunc(full_date, 'month')
        ), filled AS (
            SELECT 'month' row_type, year(c.month_start) year, month(c.month_start) month,
                   date_format(c.month_start, 'yyyy-MM') period,
                   coalesce(m.sales_count, 0L) sales_count, coalesce(m.units, 0L) units,
                   coalesce(m.revenue, cast(0 AS DECIMAL(38,2))) revenue
            FROM calendar c LEFT JOIN monthly m ON c.month_start = m.month_start
        ), periods AS (
            SELECT * FROM filled
            UNION ALL
            SELECT 'year', year, 0, cast(year AS STRING), sum(sales_count), sum(units), sum(revenue)
            FROM filled GROUP BY year
        ), previous AS (
            SELECT *, lag(revenue) OVER (PARTITION BY row_type ORDER BY year, month) previous_revenue
            FROM periods
        )
        SELECT *, CASE WHEN sales_count > 0 THEN revenue / sales_count END avg_check,
               revenue - previous_revenue revenue_change,
               CASE WHEN previous_revenue <> 0
                    THEN (cast(revenue AS DOUBLE) - cast(previous_revenue AS DOUBLE))
                         / cast(previous_revenue AS DOUBLE) * 100
               END revenue_change_pct
        FROM previous
    """,
    "report_stores": """
        WITH stores AS (
            SELECT store_name, count(*) sales_count, sum(sale_quantity) units,
                   sum(sale_total_price) revenue, avg(sale_total_price) avg_check
            FROM sales GROUP BY store_name
        )
        SELECT 'store' row_type, store_name, '' store_country, '' store_city,
               sales_count, units, revenue, avg_check,
               row_number() OVER (ORDER BY revenue DESC, store_name) revenue_rank
        FROM stores
        UNION ALL
        SELECT 'location', '', store_country, store_city,
               count(*), sum(sale_quantity), sum(sale_total_price), avg(sale_total_price), cast(NULL AS INT)
        FROM sales GROUP BY store_country, store_city
    """,
    "report_suppliers": """
        WITH suppliers AS (
            SELECT supplier_name, count(*) sales_count, sum(sale_quantity) units,
                   sum(sale_total_price) revenue, avg(product_price) avg_product_price
            FROM sales GROUP BY supplier_name
        )
        SELECT 'supplier' row_type, supplier_name, '' supplier_country,
               sales_count, units, revenue, avg_product_price,
               row_number() OVER (ORDER BY revenue DESC, supplier_name) revenue_rank
        FROM suppliers
        UNION ALL
        SELECT 'country', '', supplier_country, count(*), sum(sale_quantity),
               sum(sale_total_price), avg(product_price), cast(NULL AS INT)
        FROM sales GROUP BY supplier_country
    """,
    "report_quality": """
        SELECT p.*,
               row_number() OVER (ORDER BY avg_rating DESC, product_name) best_rating_rank,
               row_number() OVER (ORDER BY avg_rating ASC, product_name) worst_rating_rank,
               row_number() OVER (ORDER BY review_count DESC, product_name) reviews_rank,
               c.rating_sales_correlation
        FROM product_metrics p
        CROSS JOIN (SELECT corr(avg_rating, cast(units AS DOUBLE)) rating_sales_correlation FROM product_metrics) c
    """,
}


def prepare_report_views(spark):
    pg_execute(Path("/sql/checks/postgres.sql").read_text(encoding="utf-8"))
    sales = read_pg(spark, "fact_sales")
    for entity in ("customer", "product", "store", "supplier", "date"):
        dimension = read_pg(spark, "dim_" + entity)
        if entity in ("customer", "product"):
            dimension = dimension.drop("source_row_id", "source_" + entity + "_id")
        if entity == "customer":
            for column in dimension.columns:
                if column != "customer_key":
                    dimension = dimension.withColumnRenamed(column, "customer_" + column)
        sales = sales.join(dimension, entity + "_key", "inner")
    sales = sales.cache()
    if sales.count() != 10000:
        raise RuntimeError("Joining the star lost or multiplied sales")
    sales.createOrReplaceTempView("sales")
    products = spark.sql("""
        SELECT product_name, count(*) sales_count, sum(sale_quantity) units,
               sum(sale_total_price) revenue, avg(cast(product_rating AS DOUBLE)) avg_rating,
               sum(product_reviews) review_count
        FROM sales GROUP BY product_name
    """).cache()
    products.createOrReplaceTempView("product_metrics")
    return sales, products


def build_reports(spark, include_mongo=False):
    sales, products = prepare_report_views(spark)
    try:
        for name, query in REPORT_SQL.items():
            frame = spark.sql(query)
            if include_mongo and name == "report_products":
                from mongo_report import publish_mongo_products
                frame = frame.cache()
                try:
                    publish_report(name, frame)
                    publish_mongo_products(frame)
                finally:
                    frame.unpersist()
            else:
                publish_report(name, frame)
    finally:
        products.unpersist()
        sales.unpersist()


def check_reports(spark):
    """Independent PostgreSQL aggregates versus persisted ClickHouse output."""
    pg_execute(Path("/sql/checks/postgres.sql").read_text(encoding="utf-8"))
    with pg_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*), sum(sale_quantity), sum(sale_total_price) FROM fact_sales")
            count, units, revenue = cursor.fetchone()
            for table, row_types in {
                "report_products": ("product", "category"),
                "report_customers": ("customer", "country"),
                "report_time": ("month", "year"),
                "report_stores": ("store", "location"),
                "report_suppliers": ("supplier", "country"),
            }.items():
                for row_type in row_types:
                    actual = ch_query(
                        f"SELECT sum(sales_count), sum(units), sum(revenue) FROM {table} WHERE row_type = '{row_type}'"
                    ).strip().split("\t")
                    if tuple(map(Decimal, actual)) != (Decimal(count), Decimal(units), revenue):
                        raise RuntimeError(f"Totals mismatch: {table}/{row_type}: {actual}")
            cursor.execute("""
                SELECT p.product_name, count(*), sum(f.sale_quantity), sum(f.sale_total_price),
                       avg(p.product_rating), sum(p.product_reviews)
                FROM fact_sales f JOIN dim_product p USING (product_key)
                GROUP BY p.product_name ORDER BY p.product_name
            """)
            expected_products = cursor.fetchall()
            for table in ("report_products", "report_quality"):
                where = "WHERE row_type = 'product'" if table == "report_products" else ""
                result = ch_query(
                    f"SELECT product_name, sales_count, units, revenue, avg_rating, review_count "
                    f"FROM {table} {where} ORDER BY product_name FORMAT JSONEachRow"
                )
                import json
                actual_products = [json.loads(line) for line in result.splitlines()]
                if len(actual_products) != len(expected_products):
                    raise RuntimeError(f"Product count mismatch: {table}")
                for expected, actual in zip(expected_products, actual_products):
                    values = [actual[k] for k in ("product_name", "sales_count", "units", "revenue", "avg_rating", "review_count")]
                    if values[0] != expected[0] or any(
                        abs(Decimal(str(a)) - Decimal(str(e))) > Decimal("0.000001")
                        for a, e in zip(values[1:], expected[1:])
                    ):
                        raise RuntimeError(f"Product metrics mismatch: {table}: {actual}")
    print(f"CHECK OK: 6 ClickHouse reports, all 10 subtotal groups match {count} sales / {units} units / {revenue}; product ratings and reviews match PostgreSQL", flush=True)
