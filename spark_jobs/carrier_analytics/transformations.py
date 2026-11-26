from pyspark.sql import DataFrame
from pyspark.sql import functions as F

GROUP_KEYS = [
    "shipment_date",
    "carrier_id",
    "route_id",
    "origin_hub",
    "destination_hub",
]

PAYMENT_STATUSES = ["PAID", "PARTIAL", "PENDING", "OVERDUE"]


def _sum_when(condition, column: str):
    """Sum a column only for rows that match a condition."""

    return F.sum(F.when(condition, F.col(column)))


def filter_date_window(df: DataFrame, start_date, end_date) -> DataFrame:
    """
    Keep only the partitions between two dates.

    Filters on the year, month and day partition columns, so Spark only
    reads the folders in the window.

    Args:
        df: Data read from a year=/month=/day= partitioned path.
        start_date: First day of the window (inclusive).
        end_date: Last day of the window (inclusive).
    Returns:
        The rows inside the window.
    """

    partition_date = F.make_date("year", "month", "day")

    return df.filter(partition_date.between(start_date, end_date))


def prepare_shipments(shipments: DataFrame) -> DataFrame:
    """
    Select the shipment columns the job needs and add the shipment date.

    Args:
        shipments: Raw shipments.
    Returns:
        Shipments with a shipment_date column.
    """

    return shipments.select(
        "shipment_id",
        "carrier_id",
        "route_id",
        "origin_hub",
        "destination_hub",
        "weight_lbs",
        "distance_miles",
        "status",
        "sla_met",
        "created_ts",
        F.to_date("created_ts").alias("shipment_date"),
    )


def aggregate_shipments(shipments: DataFrame) -> DataFrame:
    """
    Daily shipment totals per carrier and lane.

    SLA counts DELIVERED shipments only. Cancelled shipments count in
    volume but not in weight or distance.

    Args:
        shipments: Output of prepare_shipments.
    Returns:
        One row per shipment_date, carrier and lane.
    """

    delivered = F.col("status") == "DELIVERED"
    cancelled = F.col("status") == "CANCELLED"

    return shipments.groupBy(*GROUP_KEYS).agg(
        F.count("*").alias("shipments"),
        F.count_if(delivered).alias("delivered"),
        F.count_if(cancelled).alias("cancelled"),
        F.count_if(delivered & F.col("sla_met")).alias("sla_met_delivered"),
        _sum_when(~cancelled, "weight_lbs").alias("total_weight_lbs"),
        _sum_when(~cancelled, "distance_miles").alias("total_distance_miles"),
    )


def aggregate_invoices(
        invoices: DataFrame,
        shipments: DataFrame,
) -> DataFrame:
    """
    Daily invoice totals per carrier and lane.

    Invoices join to shipments on shipment_id and take the shipment's
    carrier, lane and date. The invoice's own carrier is kept as
    billed_carrier_id and only used for the mismatch count. Invoices on
    cancelled shipments are left out of all billing metrics and summed
    into cancelled_total_amount.

    Args:
        invoices: Raw invoices.
        shipments: Output of prepare_shipments.
    Returns:
        One row per shipment_date, carrier and lane.
    """

    shipment_keys = shipments.select(
        "shipment_id",
        "created_ts",
        "status",
        *GROUP_KEYS,
    )

    joined = (
        invoices
        .withColumnRenamed("carrier_id", "billed_carrier_id")
        .join(shipment_keys, "shipment_id")
    )

    active = F.col("status") != "CANCELLED"
    disputed = active & F.col("dispute_flag")

    payment_counts = [
        F.count_if(active & (F.col("payment_status") == status))
        .alias(f"{status.lower()}_invoices")
        for status in PAYMENT_STATUSES
    ]

    return joined.groupBy(*GROUP_KEYS).agg(
        F.count_if(active).alias("invoices"),
        _sum_when(active, "base_amount").alias("base_amount"),
        _sum_when(active, "fuel_surcharge").alias("fuel_surcharge"),
        F.sum(F.when(active, F.col("fuel_surcharge") ** 2))
        .alias("fuel_surcharge_sq"),
        _sum_when(active, "detention_fee").alias("detention_fee"),
        _sum_when(active, "total_amount").alias("total_amount"),
        _sum_when(~active, "total_amount").alias("cancelled_total_amount"),
        F.count_if(disputed).alias("disputed_invoices"),
        _sum_when(disputed, "total_amount").alias("disputed_amount"),
        *payment_counts,
        F.count_if(F.col("billed_carrier_id") != F.col("carrier_id"))
        .alias("billed_carrier_mismatch"),
        F.count_if(F.col("invoice_ts") < F.col("created_ts"))
        .alias("invoice_before_shipment"),
    )


def combine_daily_totals(
        shipment_totals: DataFrame,
        invoice_totals: DataFrame,
) -> DataFrame:
    """
    Join shipment and invoice totals into one daily row.

    Every invoice row has a matching shipment row, so a left join keeps
    everything. Days with no invoices get 0 instead of null.

    Args:
        shipment_totals: Output of aggregate_shipments.
        invoice_totals: Output of aggregate_invoices.
    Returns:
        The daily carrier and lane output.
    """

    return shipment_totals.join(invoice_totals, GROUP_KEYS, "left").fillna(0)
