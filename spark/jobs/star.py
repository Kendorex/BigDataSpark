from pathlib import Path

from functools import reduce

from pyspark.sql import functions as F

from common import pg_execute, read_pg, write_pg


DIMENSIONS = {
    "customer": ["customer_first_name", "customer_last_name", "customer_age", "customer_email",
                 "customer_country", "customer_postal_code", "customer_pet_type", "customer_pet_name", "customer_pet_breed"],
    "seller": ["seller_first_name", "seller_last_name", "seller_email", "seller_country", "seller_postal_code"],
    "product": ["product_name", "product_category", "product_price", "product_quantity", "pet_category",
                "product_weight", "product_color", "product_size", "product_brand", "product_material",
                "product_description", "product_rating", "product_reviews", "product_release_date", "product_expiry_date"],
    "store": ["store_name", "store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"],
    "supplier": ["supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country"],
}


def build_star(spark):
    pg_execute(Path("/sql/postgres/03_star.sql").read_text(encoding="utf-8-sig"))
    raw = read_pg(spark, "mock_data").cache()
    count = raw.count()
    if count != 10000:
        raise RuntimeError(f"Expected 10000 raw rows, got {count}")
    invalid = raw.filter(
        F.col("sale_date").isNull() | F.col("sale_quantity").isNull()
        | (F.col("sale_quantity") <= 0) | F.col("sale_total_price").isNull()
        | (F.col("sale_total_price") < 0)
    ).count()
    if invalid:
        raise RuntimeError(f"Invalid sales: {invalid}; source is preserved for investigation")
    # Keep the schema from lab 1, including PostgreSQL-generated surrogate keys.
    # Source IDs repeat across files; source_row_id is globally unique.
    pg_execute("TRUNCATE fact_sales, map_store, map_supplier, dim_customer, dim_seller, dim_product, dim_store, dim_supplier, dim_date RESTART IDENTITY")
    for entity in ("customer", "seller"):
        dimension = raw.select(
            "source_row_id", F.col(f"sale_{entity}_id").alias(f"source_{entity}_id"),
            *[F.col(c).alias(c.removeprefix(entity + "_")) for c in DIMENSIONS[entity]],
        )
        write_pg(dimension, "dim_" + entity)
        print(f"DIMENSION dim_{entity}: {dimension.count()} rows", flush=True)
    write_pg(raw.select("source_row_id", F.col("sale_product_id").alias("source_product_id"),
                        *DIMENSIONS["product"]), "dim_product")
    for entity in ("store", "supplier"):
        write_pg(raw.select(*DIMENSIONS[entity]).distinct(), "dim_" + entity)
    dates = raw.select(F.col("sale_date").alias("full_date")).distinct().select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"), "full_date",
        F.year("full_date").alias("year"), F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"), F.dayofmonth("full_date").alias("day"),
        (F.pmod(F.dayofweek("full_date") + F.lit(5), F.lit(7)) + 1).alias("day_of_week"),
    )
    write_pg(dates, "dim_date")
    for entity in ("store", "supplier"):
        source = raw.alias("r")
        dimension = read_pg(spark, "dim_" + entity).alias("d")
        # Spark <=> (eqNullSafe) is PostgreSQL IS NOT DISTINCT FROM.
        # Ordinary '=' would lose stores with a NULL state/postal field.
        condition = reduce(lambda a, b: a & b, [
            source[c].eqNullSafe(dimension[c]) for c in DIMENSIONS[entity]
        ])
        mapping = source.join(dimension, condition, "inner").select(
            source["source_row_id"], dimension[entity + "_key"],
        )
        if mapping.count() != count:
            raise RuntimeError(f"Invalid mapping for {entity}")
        write_pg(mapping, "map_" + entity)
    facts = raw.select(
        "source_row_id", F.date_format("sale_date", "yyyyMMdd").cast("int").alias("date_key"),
        "sale_quantity", "sale_total_price",
    )
    for entity in ("customer", "seller", "product"):
        mapping = read_pg(spark, "dim_" + entity).select("source_row_id", entity + "_key")
        facts = facts.join(mapping, "source_row_id", "inner")
    for entity in ("store", "supplier"):
        facts = facts.join(read_pg(spark, "map_" + entity), "source_row_id", "inner")
    write_pg(facts, "fact_sales")
    # Check persisted output, not just the DataFrame that we intended to write.
    pg_execute(Path("/sql/checks/postgres.sql").read_text(encoding="utf-8"))
    print(f"STAR OK: {count} sales; persisted counts, units and revenue match raw", flush=True)
    raw.unpersist()
