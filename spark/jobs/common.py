import base64
import json
import math
import os
from decimal import Decimal
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg2
from pyspark.sql import SparkSession, types as T


def pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"), dbname=os.getenv("PG_DATABASE", "lab_db"),
        user=os.getenv("PG_USER", "postgres"), password=os.getenv("PG_PASSWORD", "postgres"),
    )


def pg_execute(sql):
    with pg_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)


def spark_session():
    spark = (SparkSession.builder.appName("BigDataSpark-Lab2")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def jdbc_options():
    return {
        "url": f"jdbc:postgresql://{os.getenv('PG_HOST', 'postgres')}:5432/{os.getenv('PG_DATABASE', 'lab_db')}",
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver",
    }


def read_pg(spark, table):
    return spark.read.format("jdbc").options(**jdbc_options()).option("dbtable", table).load()


def write_pg(frame, table):
    (frame.coalesce(2).write.format("jdbc").options(**jdbc_options())
     .option("dbtable", table).option("batchsize", "1000").mode("append").save())


def ch_query(query, data=b""):
    auth = base64.b64encode(
        f"{os.getenv('CH_USER', 'lab')}:{os.getenv('CH_PASSWORD', 'lab')}".encode()
    ).decode()
    params = urlencode({"database": os.getenv("CH_DATABASE", "analytics"),
                        "query": query, "wait_end_of_query": "1"})
    request = Request(os.getenv("CH_URL", "http://clickhouse:8123") + "/?" + params,
                      data=data, headers={"Authorization": "Basic " + auth}, method="POST")
    try:
        with urlopen(request, timeout=120) as response:
            return response.read().decode()
    except HTTPError as error:
        raise RuntimeError(error.read().decode()) from error


def ch_type(dtype):
    if isinstance(dtype, T.StringType):
        return "Nullable(String)"
    if isinstance(dtype, (T.IntegerType, T.LongType, T.ShortType)):
        return "Nullable(Int64)"
    if isinstance(dtype, T.DecimalType):
        return f"Nullable(Decimal({dtype.precision}, {dtype.scale}))"
    if isinstance(dtype, (T.DoubleType, T.FloatType)):
        return "Nullable(Float64)"
    raise TypeError(f"Unsupported report type: {dtype}")


def json_value(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def publish_report(name, frame):
    """Stream only aggregated results to HTTP, then swap one complete table."""
    staging = name + "_loading"
    ddl = ", ".join(f"`{f.name}` {ch_type(f.dataType)}" for f in frame.schema.fields)
    ch_query(f"DROP TABLE IF EXISTS {staging}")
    ch_query(f"CREATE TABLE {staging} ({ddl}) ENGINE = MergeTree ORDER BY tuple()")
    batch = []
    count = 0
    for row in frame.toLocalIterator():
        batch.append(json.dumps({k: json_value(v) for k, v in row.asDict().items()}, ensure_ascii=False))
        count += 1
        if len(batch) == 500:
            ch_query(f"INSERT INTO {staging} FORMAT JSONEachRow", ("\n".join(batch) + "\n").encode())
            batch.clear()
    if batch:
        ch_query(f"INSERT INTO {staging} FORMAT JSONEachRow", ("\n".join(batch) + "\n").encode())
    loaded = int(ch_query(f"SELECT count() FROM {staging}").strip())
    if loaded != count or count == 0:
        raise RuntimeError(f"{name}: expected {count} rows, loaded {loaded}")
    ch_query(f"CREATE TABLE IF NOT EXISTS {name} AS {staging}")
    ch_query(f"EXCHANGE TABLES {name} AND {staging}")
    ch_query(f"DROP TABLE {staging}")
    print(f"REPORT {name}: {count} rows published", flush=True)
