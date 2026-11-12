import logging

from pyspark.sql import SparkSession

APP_NAME = "apex-logistics-smoke-test"
SHIPMENTS_PATH = "s3://spark-apex-freight-data/raw/shipments/"
OUTPUT_PATH = "s3://spark-apex-freight-data/processed/dev/smoke_test/"
SAMPLE_ROWS = 100

logger = logging.getLogger(__name__)


def run_smoke_test(spark: SparkSession):
    """Read the raw shipments and write a small sample to processed.

    Proves the cluster can read raw/ and write processed/.

    Args:
        spark: The active SparkSession.
    """

    shipments = spark.read.parquet(SHIPMENTS_PATH)

    logger.info("Shipments schema: %s", shipments.schema.simpleString())
    logger.info("Shipments row count: %s", shipments.count())

    (
        shipments.limit(SAMPLE_ROWS)
        .write.mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    logger.info("Wrote %s rows to %s", SAMPLE_ROWS, OUTPUT_PATH)


def main():
    """Run the smoke test as a Spark job."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    try:
        run_smoke_test(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
