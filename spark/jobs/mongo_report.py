import json
import math
import os
from decimal import Decimal

from bson.decimal128 import Decimal128
from pymongo import MongoClient

from common import pg_connection


COLLECTION = "report_products"


def mongo_client():
    return MongoClient(
        os.getenv("MONGO_URI", "mongodb://lab:lab@mongodb:27017/?authSource=admin"),
        serverSelectionTimeoutMS=15000, connectTimeoutMS=15000,
    )


def bson_value(value):
    if isinstance(value, Decimal):
        return Decimal128(value)  # exact money; don't convert through float
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def validate_products(collection):
    """Check every field against independent SQL on the persisted PG star."""
    with pg_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH products AS (
                    SELECT p.product_name, count(*) sales_count, sum(f.sale_quantity) units,
                           sum(f.sale_total_price) revenue, avg(p.product_rating) avg_rating,
                           sum(p.product_reviews) review_count
                    FROM fact_sales f JOIN dim_product p USING (product_key)
                    GROUP BY p.product_name
                )
                SELECT 'product' row_type, product_name, '' product_category,
                       sales_count, units, revenue, avg_rating, review_count,
                       row_number() OVER (ORDER BY units DESC, revenue DESC, product_name) sales_rank
                FROM products
                UNION ALL
                SELECT 'category', '', p.product_category, count(*), sum(f.sale_quantity),
                       sum(f.sale_total_price), NULL::numeric, NULL::bigint, NULL::bigint
                FROM fact_sales f JOIN dim_product p USING (product_key)
                GROUP BY p.product_category
            """)
            columns = [column.name for column in cursor.description]
            expected = [dict(zip(columns, row)) for row in cursor.fetchall()]
    key_for = lambda row: (row["row_type"], row["product_name"], row["product_category"])
    actual = list(collection.find({}, {"_id": 0}))
    by_key = {key_for(row): row for row in actual}
    if not expected or len(actual) != len(by_key) or set(by_key) != {key_for(row) for row in expected}:
        raise RuntimeError("MongoDB product mart: wrong row count, duplicate or missing groups")
    for row in expected:
        found = by_key[key_for(row)]
        if set(found) != set(row) or not isinstance(found["revenue"], Decimal128):
            raise RuntimeError(f"MongoDB product mart: invalid fields/money type for {key_for(row)}")
        for column, value in row.items():
            observed = found[column]
            if isinstance(observed, Decimal128):
                observed = observed.to_decimal()
            if value is None or isinstance(value, str):
                matches = observed == value
            else:
                tolerance = Decimal("0.000001") if column == "avg_rating" else Decimal(0)
                matches = observed is not None and abs(Decimal(str(observed)) - Decimal(str(value))) <= tolerance
            if not matches:
                raise RuntimeError(f"MongoDB product mart mismatch: {key_for(row)}/{column}: {observed} != {value}")
    return len(actual)


def publish_mongo_products(frame):
    with mongo_client() as client:
        database = client[os.getenv("MONGO_DATABASE", "analytics")]
        staging = database[COLLECTION + "_loading"]
        staging.drop()
        batch = []
        for row in frame.toLocalIterator():
            document = {key: bson_value(value) for key, value in row.asDict().items()}
            document["_id"] = json.dumps(
                [document["row_type"], document["product_name"], document["product_category"]],
                ensure_ascii=False, separators=(",", ":"),
            )
            batch.append(document)
            if len(batch) == 500:
                staging.insert_many(batch)
                batch.clear()
        if batch:
            staging.insert_many(batch)
        count = validate_products(staging)
        staging.create_index([("row_type", 1), ("sales_rank", 1)])
        # Replace only after the new collection has passed field-by-field checks.
        staging.rename(COLLECTION, dropTarget=True)
    print(f"MONGO REPORT {COLLECTION}: {count} documents published", flush=True)


def build_mongo_report(spark):
    from reports import REPORT_SQL, prepare_report_views
    sales, products = prepare_report_views(spark)
    try:
        publish_mongo_products(spark.sql(REPORT_SQL[COLLECTION]))
    finally:
        products.unpersist()
        sales.unpersist()


def check_mongo_products():
    with mongo_client() as client:
        collection = client[os.getenv("MONGO_DATABASE", "analytics")][COLLECTION]
        count = validate_products(collection)
    print(f"MONGO CHECK OK: {count} documents; all fields match PostgreSQL, revenue uses Decimal128", flush=True)
