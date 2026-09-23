from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List

# Ensure Spark workers use the active virtual environment.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
VALIDATED_ROOT = PROJECT_ROOT / "data" / "validated"
AUDIT_ROOT = PROJECT_ROOT / "data" / "validation_audit"
DATA_QUALITY_ROOT = PROJECT_ROOT / "data" / "data_quality_report"


PRIMARY_KEYS: Dict[str, List[str]] = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "order_items": ["order_item_id"],
    "payments": ["payment_id"],
}


REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id", "customer_id"],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
    ],
    "payments": ["payment_id", "order_id"],
}


TIMESTAMP_COLUMNS: Dict[str, List[str]] = {
    "customers": ["validation_timestamp"],
    "products": ["validation_timestamp"],
    "orders": ["order_date", "validation_timestamp"],
    "order_items": ["validation_timestamp"],
    "payments": ["payment_date", "validation_timestamp"],
}


ALLOWED_VALUES = {
    "orders": {
        "order_status": [
            "Pending",
            "Confirmed",
            "Shipped",
            "Delivered",
            "Cancelled",
            "Returned",
        ]
    },
    "payments": {
        "payment_status": [
            "Pending",
            "Success",
            "Completed",
            "Failed",
            "Refunded",
        ],
        "payment_method": [
            "Credit Card",
            "Debit Card",
            "UPI",
            "Net Banking",
            "Cash on Delivery",
            "Wallet",
        ],
    },
}


def create_spark_session() -> SparkSession:
    """Create the Spark session used by the validation framework."""

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("RetailAnalyticsBronzeValidation")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.legacy.timeParserPolicy",
            "CORRECTED",
        )
        .getOrCreate()
    )


def empty_string_array():
    """Return an empty Spark array<string> expression."""

    return F.expr("array()").cast("array<string>")


def add_error(
    dataframe: DataFrame,
    condition,
    message: str,
) -> DataFrame:
    """Append a validation error when the condition is true."""

    new_error = F.when(
        condition,
        F.array(F.lit(message)),
    ).otherwise(empty_string_array())

    return dataframe.withColumn(
        "_validation_errors",
        F.concat(F.col("_validation_errors"), new_error),
    )


def is_missing(column_name: str):
    """Check whether a string-like value is missing."""

    normalized_value = F.lower(F.trim(F.col(column_name).cast("string")))

    return (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name).cast("string")) == "")
        | normalized_value.isin("null", "none", "nan")
    )


def validate_required_columns(
    dataframe: DataFrame,
    dataset_name: str,
) -> DataFrame:
    """Validate mandatory columns."""

    for column_name in REQUIRED_COLUMNS.get(dataset_name, []):
        dataframe = add_error(
            dataframe,
            is_missing(column_name),
            f"MISSING_REQUIRED_FIELD:{column_name}",
        )

    return dataframe


def validate_duplicate_keys(
    dataframe: DataFrame,
    dataset_name: str,
) -> DataFrame:
    """Mark every row whose primary key is duplicated."""

    primary_keys = PRIMARY_KEYS[dataset_name]

    duplicate_keys = (
        dataframe
        .groupBy(*primary_keys)
        .count()
        .filter(F.col("count") > 1)
        .select(*primary_keys)
        .withColumn("_is_duplicate", F.lit(True))
    )

    dataframe = dataframe.join(
        duplicate_keys,
        on=primary_keys,
        how="left",
    )

    dataframe = add_error(
        dataframe,
        F.col("_is_duplicate") == F.lit(True),
        "DUPLICATE_PRIMARY_KEY",
    )

    return dataframe.drop("_is_duplicate")


def validate_timestamps(
    dataframe: DataFrame,
    dataset_name: str,
) -> DataFrame:
    """
    Validate source timestamps without modifying Bronze values.

    Supported source formats:
    - yyyy-MM-dd HH:mm:ss
    - yyyy-MM-dd HH:mm:ss.SSSSSS
    """

    for column_name in TIMESTAMP_COLUMNS.get(dataset_name, []):
        if column_name not in dataframe.columns:
            continue

        source_value = F.trim(
            F.col(column_name).cast("string")
        )

        has_value = (
            F.col(column_name).isNotNull()
            & (source_value != "")
            & (~F.lower(source_value).isin(
                "null",
                "none",
                "nan",
            ))
        )

        parsed_timestamp = F.coalesce(
            F.to_timestamp(
                source_value,
                "yyyy-MM-dd HH:mm:ss",
            ),
            F.to_timestamp(
                source_value,
                "yyyy-MM-dd HH:mm:ss.SSSSSS",
            ),
        )

        dataframe = add_error(
            dataframe,
            has_value & parsed_timestamp.isNull(),
            f"INVALID_TIMESTAMP:{column_name}",
        )

    return dataframe


def validate_customer_rules(dataframe: DataFrame) -> DataFrame:
    """Apply validations specific to customers."""

    if "email" in dataframe.columns:
        email_has_value = ~is_missing("email")

        valid_email_pattern = (
            r"^[A-Za-z0-9._%+-]+@"
            r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        )

        dataframe = add_error(
            dataframe,
            email_has_value
            & (~F.col("email").rlike(valid_email_pattern)),
            "INVALID_EMAIL_FORMAT",
        )

    return dataframe


def validate_product_rules(dataframe: DataFrame) -> DataFrame:
    """Apply validations specific to products."""

    if "price" in dataframe.columns:
        dataframe = add_error(
            dataframe,
            F.col("price").isNotNull()
            & (F.col("price") < 0),
            "NEGATIVE_PRICE",
        )

    if "stock_quantity" in dataframe.columns:
        dataframe = add_error(
            dataframe,
            F.col("stock_quantity").isNotNull()
            & (F.col("stock_quantity") < 0),
            "NEGATIVE_STOCK_QUANTITY",
        )

    return dataframe


def validate_order_item_rules(dataframe: DataFrame) -> DataFrame:
    """Apply validations specific to order items."""

    if "quantity" in dataframe.columns:
        dataframe = add_error(
            dataframe,
            F.col("quantity").isNull()
            | (F.col("quantity") <= 0),
            "INVALID_QUANTITY",
        )

    if "unit_price" in dataframe.columns:
        dataframe = add_error(
            dataframe,
            F.col("unit_price").isNull()
            | (F.col("unit_price") < 0),
            "INVALID_UNIT_PRICE",
        )

    if "discount_percentage" in dataframe.columns:
        dataframe = add_error(
            dataframe,
            F.col("discount_percentage").isNotNull()
            & (
                (F.col("discount_percentage") < 0)
                | (F.col("discount_percentage") > 100)
            ),
            "INVALID_DISCOUNT_PERCENTAGE",
        )

    return dataframe


def validate_payment_rules(dataframe: DataFrame) -> DataFrame:
    """Apply validations specific to payments."""

    if "payment_amount" in dataframe.columns:
        dataframe = add_error(
            dataframe,
            F.col("payment_amount").isNull()
            | (F.col("payment_amount") < 0),
            "INVALID_PAYMENT_AMOUNT",
        )

    return dataframe


def validate_allowed_values(
    dataframe: DataFrame,
    dataset_name: str,
) -> DataFrame:
    """Validate categorical fields against accepted values."""

    dataset_rules = ALLOWED_VALUES.get(dataset_name, {})

    for column_name, accepted_values in dataset_rules.items():
        if column_name not in dataframe.columns:
            continue

        dataframe = add_error(
            dataframe,
            ~is_missing(column_name)
            & (~F.col(column_name).isin(accepted_values)),
            f"INVALID_VALUE:{column_name}",
        )

    return dataframe


def add_reference_marker(
    reference_df: DataFrame,
    key_column: str,
    marker_column: str,
) -> DataFrame:
    """Prepare reference keys for referential-integrity joins."""

    return (
        reference_df
        .select(key_column)
        .filter(~is_missing(key_column))
        .distinct()
        .withColumn(marker_column, F.lit(True))
    )


def validate_referential_integrity(
    dataframe: DataFrame,
    dataset_name: str,
    bronze_dataframes: Dict[str, DataFrame],
) -> DataFrame:
    """Check relationships between retail datasets."""

    if dataset_name == "orders":
        customer_keys = add_reference_marker(
            bronze_dataframes["customers"],
            "customer_id",
            "_customer_exists",
        )

        dataframe = dataframe.join(
            customer_keys,
            on="customer_id",
            how="left",
        )

        dataframe = add_error(
            dataframe,
            ~is_missing("customer_id")
            & F.col("_customer_exists").isNull(),
            "CUSTOMER_NOT_FOUND",
        )

        dataframe = dataframe.drop("_customer_exists")

    elif dataset_name == "order_items":
        order_keys = add_reference_marker(
            bronze_dataframes["orders"],
            "order_id",
            "_order_exists",
        )

        product_keys = add_reference_marker(
            bronze_dataframes["products"],
            "product_id",
            "_product_exists",
        )

        dataframe = dataframe.join(
            order_keys,
            on="order_id",
            how="left",
        )

        dataframe = add_error(
            dataframe,
            ~is_missing("order_id")
            & F.col("_order_exists").isNull(),
            "ORDER_NOT_FOUND",
        )

        dataframe = dataframe.drop("_order_exists")

        dataframe = dataframe.join(
            product_keys,
            on="product_id",
            how="left",
        )

        dataframe = add_error(
            dataframe,
            ~is_missing("product_id")
            & F.col("_product_exists").isNull(),
            "PRODUCT_NOT_FOUND",
        )

        dataframe = dataframe.drop("_product_exists")

    elif dataset_name == "payments":
        order_keys = add_reference_marker(
            bronze_dataframes["orders"],
            "order_id",
            "_order_exists",
        )

        dataframe = dataframe.join(
            order_keys,
            on="order_id",
            how="left",
        )

        dataframe = add_error(
            dataframe,
            ~is_missing("order_id")
            & F.col("_order_exists").isNull(),
            "ORDER_NOT_FOUND",
        )

        dataframe = dataframe.drop("_order_exists")

    return dataframe


def validate_dataset(
    dataframe: DataFrame,
    dataset_name: str,
    bronze_dataframes: Dict[str, DataFrame],
    rejected_orders: DataFrame | None = None,
) -> DataFrame:
    """Run all validations for one Bronze dataset."""

    dataframe = dataframe.withColumn(
        "_validation_errors",
        empty_string_array(),
    )

    dataframe = validate_required_columns(
        dataframe,
        dataset_name,
    )

    dataframe = validate_duplicate_keys(
        dataframe,
        dataset_name,
    )

    dataframe = validate_timestamps(
        dataframe,
        dataset_name,
    )

    dataframe = validate_allowed_values(
        dataframe,
        dataset_name,
    )

    dataframe = validate_referential_integrity(
        dataframe,
        dataset_name,
        bronze_dataframes,
    )

    if dataset_name == "customers":
        dataframe = validate_customer_rules(dataframe)

    elif dataset_name == "products":
        dataframe = validate_product_rules(dataframe)

    elif dataset_name == "order_items":
        dataframe = validate_order_item_rules(dataframe)

    elif dataset_name == "payments":
        dataframe = validate_payment_rules(dataframe)

    # Cascade: an item or payment whose parent order was quarantined
    # cannot be loaded either, otherwise it becomes an orphan fact.
    if (
        rejected_orders is not None
        and dataset_name in ("order_items", "payments")
    ):
        dataframe = dataframe.join(
            F.broadcast(rejected_orders),
            on="order_id",
            how="left",
        )
        dataframe = add_error(
            dataframe,
            F.col("_parent_rejected").isNotNull(),
            "PARENT_ORDER_QUARANTINED",
        ).drop("_parent_rejected")

    dataframe = dataframe.withColumn(
        "_validation_status",
        F.when(
            F.size(F.col("_validation_errors")) == 0,
            F.lit("VALID"),
        ).otherwise(F.lit("INVALID")),
    )

    dataframe = dataframe.withColumn(
    "_validation_reason",
    F.when(
        F.size(F.col("_validation_errors")) == 0,
        F.lit(None).cast("string"),
    ).otherwise(
        F.concat_ws(
            "; ",
            F.col("_validation_errors"),
        )
    ),
)

    dataframe = dataframe.withColumn(
        "_validated_at",
        F.current_timestamp(),
    )

    return dataframe


def read_bronze_data(
    spark: SparkSession,
    batch_date: str,
) -> Dict[str, DataFrame]:
    """Read every Bronze dataset for a batch."""

    bronze_dataframes: Dict[str, DataFrame] = {}

    for dataset_name in PRIMARY_KEYS:
        input_path = (
            BRONZE_ROOT
            / f"batch_date={batch_date}"
            / dataset_name
        )

        if not input_path.exists():
            raise FileNotFoundError(
                f"Bronze dataset not found: {input_path}"
            )

        bronze_dataframes[dataset_name] = (
            spark.read.parquet(str(input_path))
        )

    return bronze_dataframes


def write_validation_results(
    dataframe: DataFrame,
    dataset_name: str,
    batch_date: str,
) -> Dict[str, object]:
    """Write valid and quarantined records and return summary metrics."""

    output_root = (
        VALIDATED_ROOT
        / f"batch_date={batch_date}"
        / dataset_name
    )

    valid_output_path = output_root / "valid"
    quarantine_output_path = output_root / "quarantine"

    valid_df = dataframe.filter(
        F.col("_validation_status") == "VALID"
    )

    quarantine_df = dataframe.filter(
        F.col("_validation_status") == "INVALID"
    )

    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()
    total_count = valid_count + quarantine_count

    if total_count == 0:
        pass_percentage = 0.0
        failure_percentage = 0.0
    else:
        pass_percentage = round(
            (valid_count / total_count) * 100,
            2,
        )

        failure_percentage = round(
            (quarantine_count / total_count) * 100,
            2,
        )

    (
        valid_df
        .drop(
            "_validation_errors",
            "_validation_status",
            "_validation_reason",
        )
        .write
        .mode("overwrite")
        .parquet(str(valid_output_path))
    )

    (
        quarantine_df
        .write
        .mode("overwrite")
        .parquet(str(quarantine_output_path))
    )

    return {
        "batch_date": batch_date,
        "dataset_name": dataset_name,
        "total_records": total_count,
        "valid_records": valid_count,
        "invalid_records": quarantine_count,
        "pass_percentage": pass_percentage,
        "failure_percentage": failure_percentage,
        "validation_status": (
            "SUCCESS"
            if quarantine_count == 0
            else "COMPLETED_WITH_INVALID_RECORDS"
        ),
    }


def create_rule_failure_report(
    dataframe: DataFrame,
    dataset_name: str,
    batch_date: str,
) -> DataFrame:
    """
    Create rule-wise data quality metrics.

    One invalid record can appear against multiple rules if it failed
    multiple validations.
    """

    total_records = dataframe.count()

    rule_failures = (
        dataframe
        .filter(F.size(F.col("_validation_errors")) > 0)
        .select(
            F.explode(
                F.col("_validation_errors")
            ).alias("validation_rule")
        )
        .groupBy("validation_rule")
        .agg(
            F.count(F.lit(1)).alias("failed_records")
        )
    )

    return (
        rule_failures
        .withColumn(
            "batch_date",
            F.lit(batch_date),
        )
        .withColumn(
            "dataset_name",
            F.lit(dataset_name),
        )
        .withColumn(
            "total_records",
            F.lit(total_records),
        )
        .withColumn(
            "failure_percentage",
            F.when(
                F.col("total_records") == 0,
                F.lit(0.0),
            ).otherwise(
                F.round(
                    (
                        F.col("failed_records")
                        / F.col("total_records")
                    ) * 100,
                    2,
                )
            ),
        )
        .withColumn(
            "report_generated_at",
            F.current_timestamp(),
        )
        .select(
            "batch_date",
            "dataset_name",
            "validation_rule",
            "total_records",
            "failed_records",
            "failure_percentage",
            "report_generated_at",
        )
    )


def union_dataframes(
    dataframes: List[DataFrame],
) -> DataFrame | None:
    """Union DataFrames with matching columns."""

    if not dataframes:
        return None

    combined_df = dataframes[0]

    for dataframe in dataframes[1:]:
        combined_df = combined_df.unionByName(
            dataframe,
            allowMissingColumns=True,
        )

    return combined_df

def run_validation(batch_date: str) -> None:
    """Run validation for all Bronze datasets."""

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    audit_records: List[Dict[str, object]] = []
    rule_reports: List[DataFrame] = []

    try:
        bronze_dataframes = read_bronze_data(
            spark,
            batch_date,
        )

        print("=" * 110)
        print(
            f"BRONZE VALIDATION — BATCH DATE: {batch_date}"
        )
        print("=" * 110)

        rejected_orders = None

        for dataset_name, bronze_df in bronze_dataframes.items():
            validated_df = validate_dataset(
                dataframe=bronze_df,
                dataset_name=dataset_name,
                bronze_dataframes=bronze_dataframes,
                rejected_orders=rejected_orders,
            ).cache()

            if dataset_name == "orders":
                rejected_orders = (
                    validated_df
                    .filter(F.size(F.col("_validation_errors")) > 0)
                    .select("order_id")
                    .distinct()
                    .withColumn("_parent_rejected", F.lit(True))
                    .cache()
                )

            audit_record = write_validation_results(
                dataframe=validated_df,
                dataset_name=dataset_name,
                batch_date=batch_date,
            )

            audit_records.append(audit_record)

            rule_report_df = create_rule_failure_report(
                dataframe=validated_df,
                dataset_name=dataset_name,
                batch_date=batch_date,
            )

            rule_reports.append(rule_report_df)

            print(
                f"{dataset_name:<13} "
                f"Total: {audit_record['total_records']:>8} | "
                f"Valid: {audit_record['valid_records']:>8} | "
                f"Invalid: {audit_record['invalid_records']:>8} | "
                f"Pass: {audit_record['pass_percentage']:>6.2f}% | "
                f"Status: {audit_record['validation_status']}"
            )

            validated_df.unpersist()

        audit_df = spark.createDataFrame(audit_records)

        audit_output_path = (
            AUDIT_ROOT
            / f"batch_date={batch_date}"
        )

        (
            audit_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(str(audit_output_path))
        )

        combined_rule_report = union_dataframes(
            rule_reports
        )

        data_quality_output_path = (
            DATA_QUALITY_ROOT
            / f"batch_date={batch_date}"
        )

        if combined_rule_report is not None:
            (
                combined_rule_report
                .orderBy(
                    "dataset_name",
                    F.desc("failed_records"),
                )
                .coalesce(1)
                .write
                .mode("overwrite")
                .option("header", True)
                .csv(str(data_quality_output_path))
            )

        print("=" * 110)
        print("VALIDATION COMPLETED")
        print(
            f"Validated output:   {VALIDATED_ROOT}"
        )
        print(
            f"Audit output:       {audit_output_path}"
        )
        print(
            f"Data quality report:{data_quality_output_path}"
        )
        print("=" * 110)

    finally:
        spark.stop()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Retail Analytics Bronze datasets."
    )

    parser.add_argument(
        "--batch-date",
        required=True,
        help="Batch date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    run_validation(arguments.batch_date)


if __name__ == "__main__":
    main()
