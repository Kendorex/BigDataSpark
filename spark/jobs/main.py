import argparse

from common import spark_session
from reports import build_reports, check_reports
from mongo_report import build_mongo_report, check_mongo_products
from star import build_star


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=["all", "star", "reports", "mongo", "check"], default="all", nargs="?")
    parser.add_argument("--mongo", action="store_true", help="Also publish/check the product mart in MongoDB (all, reports, check)")
    args = parser.parse_args()
    if args.mongo and args.stage not in ("all", "reports", "check"):
        parser.error("--mongo can only be used with all, reports or check")
    spark = spark_session()
    try:
        if args.stage in ("all", "star"):
            build_star(spark)
        if args.stage in ("all", "reports"):
            build_reports(spark, include_mongo=args.mongo)
        if args.stage == "mongo":
            build_mongo_report(spark)
        if args.stage in ("all", "reports", "check"):
            check_reports(spark)
        if args.mongo or args.stage == "mongo":
            check_mongo_products()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
