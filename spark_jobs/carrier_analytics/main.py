import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from transformations import (aggregate_invoices, aggregate_shipments,
                             combine_daily_totals, filter_date_window,
                             prepare_shipments)

APP_NAME = "apex-logistics-carrier-analytics"
RAW_PATH = "s3://spark-apex-freight-data/raw"
LOOKBACK_DAYS = 45

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Read the job arguments passed by spark-submit.

    Args:
        argv: Arguments to parse. Defaults to the command line.
    Returns:
        The parsed arguments.
    """

    parser = argparse.ArgumentParser(
        description="Build daily carrier and lane totals.",
    )
    parser.add_argument(
        "--run-date",
        required=True,
        type=date.fromisoformat,
        help="Last day to process, as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Delta table path for the daily totals.",
    )
    parser.add_argument(
        "--raw-path",
        default=RAW_PATH,
        help="Folder that holds raw shipments/ and invoices/.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=LOOKBACK_DAYS,
        help="Number of days to recompute, ending on the run date.",
    )

    return parser.parse_args(argv)


def get_date_window(run_date: date, lookback_days: int) -> tuple[date, date]:
    """
    Get the first and last day to recompute.

    Args:
        run_date: Last day of the window.
        lookback_days: Number of days in the window, run date included.
    Returns:
        The start and end dates of the window.
    """

    start_date = run_date - timedelta(days=lookback_days - 1)

    return start_date, run_date


def build_daily_totals(
        spark: SparkSession,
        raw_path: str,
        start_date: date,
        end_date: date,
) -> DataFrame:
    """
    Read the raw data in the window and build the daily totals.

    Shipments and invoices use the same window because no invoice is
    dated before its shipment.

    Args:
        spark: The active SparkSession.
        raw_path: Folder that holds raw shipments/ and invoices/.
        start_date: First day of the window.
        end_date: Last day of the window.
    Returns:
        One row per shipment_date, carrier and lane.
    """

    shipments = filter_date_window(
        spark.read.parquet(f"{raw_path}/shipments/"),
        start_date,
        end_date,
    )
    invoices = filter_date_window(
        spark.read.parquet(f"{raw_path}/invoices/"),
        start_date,
        end_date,
    )

    shipments = prepare_shipments(shipments)

    return combine_daily_totals(
        aggregate_shipments(shipments),
        aggregate_invoices(invoices, shipments),
    )


def write_daily_totals(
        daily_totals: DataFrame,
        output_path: str,
        start_date: date,
        end_date: date,
):
    """
    Overwrite the window in the Delta table.

    replaceWhere only replaces the days in the window. Older days stay
    as they are. Repartitioning by shipment_date writes one file per
    day instead of many small ones.

    Args:
        daily_totals: Output of build_daily_totals.
        output_path: Delta table path.
        start_date: First day of the window.
        end_date: Last day of the window.
    """

    window = (
        f"shipment_date >= DATE'{start_date}' "
        f"AND shipment_date <= DATE'{end_date}'"
    )

    (
        daily_totals.repartition("shipment_date")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", window)
        .partitionBy("shipment_date")
        .save(output_path)
    )


def main():
    """Run the carrier analytics job."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    args = parse_args()
    start_date, end_date = get_date_window(args.run_date, args.lookback_days)

    spark = (
        SparkSession.builder.appName(APP_NAME)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    try:
        logger.info("Processing %s to %s", start_date, end_date)

        daily_totals = build_daily_totals(
            spark,
            args.raw_path,
            start_date,
            end_date,
        )
        write_daily_totals(
            daily_totals,
            args.output_path,
            start_date,
            end_date,
        )

        logger.info("Wrote daily totals to %s", args.output_path)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
