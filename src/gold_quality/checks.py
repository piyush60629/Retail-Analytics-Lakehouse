from __future__ import annotations

from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CheckResult = Dict[str, object]


def create_check_result(
    check_name: str,
    table_name: str,
    status: str,
    failed_count: int,
    description: str,
) -> CheckResult:
    """
    Create a standard quality-check result.
    """

    return {
        "check_name": check_name,
        "table_name": table_name,
        "status": status,
        "failed_count": int(failed_count),
        "description": description,
    }


def validate_required_columns(
    dataframe: DataFrame,
    required_columns: List[str],
    dataset_name: str,
) -> None:
    """
    Raise an error when required columns are missing.
    """

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{missing_columns}"
        )


def check_not_null(
    dataframe: DataFrame,
    table_name: str,
    columns: List[str],
) -> List[CheckResult]:
    """
    Validate that important columns do not contain nulls.
    """

    validate_required_columns(
        dataframe=dataframe,
        required_columns=columns,
        dataset_name=table_name,
    )

    results: List[CheckResult] = []

    for column_name in columns:
        failed_count = (
            dataframe
            .filter(
                F.col(column_name).isNull()
            )
            .count()
        )

        status = (
            "PASS"
            if failed_count == 0
            else "FAIL"
        )

        results.append(
            create_check_result(
                check_name=(
                    f"not_null_{column_name}"
                ),
                table_name=table_name,
                status=status,
                failed_count=failed_count,
                description=(
                    f"Column {column_name} must not "
                    "contain null values."
                ),
            )
        )

    return results


def check_unique_key(
    dataframe: DataFrame,
    table_name: str,
    key_columns: List[str],
) -> CheckResult:
    """
    Validate that a primary or composite key is unique.
    """

    validate_required_columns(
        dataframe=dataframe,
        required_columns=key_columns,
        dataset_name=table_name,
    )

    failed_count = (
        dataframe
        .groupBy(*key_columns)
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    return create_check_result(
        check_name="unique_key",
        table_name=table_name,
        status=status,
        failed_count=failed_count,
        description=(
            "Key columns must uniquely identify "
            f"each record: {key_columns}"
        ),
    )


def check_foreign_key(
    fact_dataframe: DataFrame,
    dimension_dataframe: DataFrame,
    fact_table_name: str,
    dimension_table_name: str,
    fact_key: str,
    dimension_key: str,
) -> CheckResult:
    """
    Validate that every non-null fact foreign key exists
    in its related dimension.
    """

    validate_required_columns(
        dataframe=fact_dataframe,
        required_columns=[fact_key],
        dataset_name=fact_table_name,
    )

    validate_required_columns(
        dataframe=dimension_dataframe,
        required_columns=[dimension_key],
        dataset_name=dimension_table_name,
    )

    fact_keys = (
        fact_dataframe
        .select(
            F.col(fact_key).alias(
                "fact_foreign_key"
            )
        )
        .filter(
            F.col(
                "fact_foreign_key"
            ).isNotNull()
        )
        .dropDuplicates()
    )

    dimension_keys = (
        dimension_dataframe
        .select(
            F.col(dimension_key).alias(
                "dimension_primary_key"
            )
        )
        .filter(
            F.col(
                "dimension_primary_key"
            ).isNotNull()
        )
        .dropDuplicates()
    )

    failed_count = (
        fact_keys
        .join(
            dimension_keys,
            fact_keys["fact_foreign_key"]
            == dimension_keys[
                "dimension_primary_key"
            ],
            "left_anti",
        )
        .count()
    )

    status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    return create_check_result(
        check_name=(
            f"foreign_key_{fact_key}"
        ),
        table_name=fact_table_name,
        status=status,
        failed_count=failed_count,
        description=(
            f"Every {fact_table_name}.{fact_key} "
            f"must exist in "
            f"{dimension_table_name}.{dimension_key}."
        ),
    )


def check_row_count_reconciliation(
    source_dataframe: DataFrame,
    target_dataframe: DataFrame,
    source_name: str,
    target_name: str,
) -> CheckResult:
    """
    Compare source and target row counts.
    """

    source_count = source_dataframe.count()
    target_count = target_dataframe.count()

    failed_count = abs(
        source_count - target_count
    )

    status = (
        "PASS"
        if source_count == target_count
        else "FAIL"
    )

    return create_check_result(
        check_name="row_count_reconciliation",
        table_name=target_name,
        status=status,
        failed_count=failed_count,
        description=(
            f"{source_name} rows={source_count}; "
            f"{target_name} rows={target_count}."
        ),
    )


def check_non_negative_columns(
    dataframe: DataFrame,
    table_name: str,
    columns: List[str],
) -> List[CheckResult]:
    """
    Validate that numeric amount columns are not negative.
    """

    results: List[CheckResult] = []

    existing_columns = [
        column_name
        for column_name in columns
        if column_name in dataframe.columns
    ]

    for column_name in existing_columns:
        failed_count = (
            dataframe
            .filter(
                F.col(column_name) < 0
            )
            .count()
        )

        status = (
            "PASS"
            if failed_count == 0
            else "FAIL"
        )

        results.append(
            create_check_result(
                check_name=(
                    f"non_negative_{column_name}"
                ),
                table_name=table_name,
                status=status,
                failed_count=failed_count,
                description=(
                    f"Column {column_name} must not "
                    "contain negative values."
                ),
            )
        )

    return results


def check_positive_column(
    dataframe: DataFrame,
    table_name: str,
    column_name: str,
) -> CheckResult:
    """
    Validate that a numeric column is greater than zero.
    """

    validate_required_columns(
        dataframe=dataframe,
        required_columns=[column_name],
        dataset_name=table_name,
    )

    failed_count = (
        dataframe
        .filter(
            F.col(column_name).isNull()
            | (F.col(column_name) <= 0)
        )
        .count()
    )

    status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    return create_check_result(
        check_name=(
            f"positive_{column_name}"
        ),
        table_name=table_name,
        status=status,
        failed_count=failed_count,
        description=(
            f"Column {column_name} must be "
            "greater than zero."
        ),
    )


def check_sales_amount_equation(
    dataframe: DataFrame,
    table_name: str = "fact_sales",
) -> CheckResult:
    """
    Validate:

    net_amount =
        gross_amount
        - discount_amount
        + tax_amount
    """

    required_columns = [
        "gross_amount",
        "discount_amount",
        "tax_amount",
        "net_amount",
    ]

    validate_required_columns(
        dataframe=dataframe,
        required_columns=required_columns,
        dataset_name=table_name,
    )

    expected_net_amount = F.round(
        F.col("gross_amount")
        - F.col("discount_amount")
        + F.col("tax_amount"),
        2,
    )

    actual_net_amount = F.round(
        F.col("net_amount"),
        2,
    )

    failed_count = (
        dataframe
        .filter(
            expected_net_amount
            != actual_net_amount
        )
        .count()
    )

    status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    return create_check_result(
        check_name="sales_amount_equation",
        table_name=table_name,
        status=status,
        failed_count=failed_count,
        description=(
            "net_amount must equal gross_amount "
            "- discount_amount + tax_amount."
        ),
    )


def check_payment_amount_equation(
    dataframe: DataFrame,
    table_name: str = "fact_payment",
) -> CheckResult:
    """
    Validate:

    net_payment_amount =
        payment_amount - refund_amount
    """

    required_columns = [
        "payment_amount",
        "refund_amount",
        "net_payment_amount",
    ]

    validate_required_columns(
        dataframe=dataframe,
        required_columns=required_columns,
        dataset_name=table_name,
    )

    expected_amount = F.round(
        F.col("payment_amount")
        - F.col("refund_amount"),
        2,
    )

    actual_amount = F.round(
        F.col("net_payment_amount"),
        2,
    )

    failed_count = (
        dataframe
        .filter(
            expected_amount != actual_amount
        )
        .count()
    )

    status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    return create_check_result(
        check_name="payment_amount_equation",
        table_name=table_name,
        status=status,
        failed_count=failed_count,
        description=(
            "net_payment_amount must equal "
            "payment_amount - refund_amount."
        ),
    )


def check_current_scd_uniqueness(
    dataframe: DataFrame,
    table_name: str,
    business_key: str,
) -> CheckResult:
    """
    Validate that each business key has no more than one
    current SCD Type 2 record.
    """

    validate_required_columns(
        dataframe=dataframe,
        required_columns=[
            business_key,
            "is_current",
        ],
        dataset_name=table_name,
    )

    failed_count = (
        dataframe
        .filter(
            F.col("is_current") == F.lit(True)
        )
        .groupBy(business_key)
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    return create_check_result(
        check_name="current_scd_uniqueness",
        table_name=table_name,
        status=status,
        failed_count=failed_count,
        description=(
            f"Each {business_key} must have at most "
            "one current SCD2 record."
        ),
    )


def check_scd_date_range(
    dataframe: DataFrame,
    table_name: str,
) -> CheckResult:
    """
    Validate SCD Type 2 effective dates.

    Rules:
        1. effective_from must not be null.
        2. Current records may have effective_to = null.
        3. Historical records must have effective_to.
        4. effective_to cannot be before effective_from.
    """

    validate_required_columns(
        dataframe=dataframe,
        required_columns=[
            "effective_from",
            "effective_to",
            "is_current",
        ],
        dataset_name=table_name,
    )

    checked_dataframe = dataframe.withColumn(
        "_is_current_boolean",
        F.col("is_current").cast("boolean"),
    )

    invalid_records = checked_dataframe.filter(
        # effective_from is always required
        F.col("effective_from").isNull()

        # Historical records require effective_to
        | (
            (
                F.col("_is_current_boolean")
                == F.lit(False)
            )
            & F.col("effective_to").isNull()
        )

        # effective_to cannot be earlier than effective_from
        | (
            F.col("effective_to").isNotNull()
            & (
                F.col("effective_to")
                < F.col("effective_from")
            )
        )
    )

    failed_count = invalid_records.count()

    status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    return create_check_result(
        check_name="valid_scd_date_range",
        table_name=table_name,
        status=status,
        failed_count=failed_count,
        description=(
            "effective_from must exist; current records "
            "may have null effective_to; historical records "
            "must have a valid effective_to."
        ),
    )


def check_date_key_consistency(
    dataframe: DataFrame,
    table_name: str,
    date_column: str,
    date_key_column: str,
) -> CheckResult:
    """
    Validate that a date key matches its date value.
    """

    validate_required_columns(
        dataframe=dataframe,
        required_columns=[
            date_column,
            date_key_column,
        ],
        dataset_name=table_name,
    )

    expected_key = F.date_format(
        F.to_date(
            F.col(date_column)
        ),
        "yyyyMMdd",
    ).cast("integer")

    failed_count = (
        dataframe
        .filter(
            F.col(date_column).isNull()
            | F.col(date_key_column).isNull()
            | (
                expected_key
                != F.col(date_key_column)
            )
        )
        .count()
    )

    status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    return create_check_result(
        check_name=(
            f"date_key_consistency_"
            f"{date_key_column}"
        ),
        table_name=table_name,
        status=status,
        failed_count=failed_count,
        description=(
            f"{date_key_column} must match "
            f"{date_column} in yyyyMMdd format."
        ),
    )


def check_current_scd_end_date(
    dataframe: DataFrame,
    table_name: str,
) -> CheckResult:
    """
    Validate that current SCD2 records have no end date.

    Current records:
        is_current = true
        effective_to = null
    """

    validate_required_columns(
        dataframe=dataframe,
        required_columns=[
            "is_current",
            "effective_to",
        ],
        dataset_name=table_name,
    )

    failed_count = (
        dataframe
        .filter(
            (
                F.col("is_current")
                == F.lit(True)
            )
            & F.col("effective_to").isNotNull()
        )
        .count()
    )

    status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    return create_check_result(
        check_name="current_scd_end_date",
        table_name=table_name,
        status=status,
        failed_count=failed_count,
        description=(
            "Current SCD2 records must have a null "
            "effective_to value."
        ),
    )