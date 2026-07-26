from __future__ import annotations

import re
from datetime import datetime

import pandas as pd


EMAIL_PATTERN = re.compile(
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)


VALID_CUSTOMER_SEGMENTS = {
    "Regular",
    "Silver",
    "Gold",
    "Platinum",
}

VALID_PRODUCT_CATEGORIES = {
    "Electronics",
    "Fashion",
    "Furniture",
    "Books",
    "Sports",
    "Beauty",
    "Home",
    "Toys",
}

VALID_ORDER_STATUSES = {
    "Delivered",
    "Cancelled",
    "Pending",
    "Returned",
    "Shipped",
}

VALID_PAYMENT_METHODS = {
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Wallet",
}

VALID_PAYMENT_STATUSES = {
    "Success",
    "Failed",
    "Refunded",
    "Pending",
}


def append_rejection_reason(
    dataframe: pd.DataFrame,
    condition: pd.Series,
    reason: str,
) -> None:
    """
    Append a rejection reason to rows matching a condition.

    Multiple validation failures are stored using a pipe separator.
    """

    existing_reasons = dataframe.loc[
        condition,
        "rejection_reason",
    ].fillna("")

    dataframe.loc[
        condition,
        "rejection_reason",
    ] = existing_reasons.apply(
        lambda current_reason: (
            f"{current_reason} | {reason}"
            if current_reason
            else reason
        )
    )


def initialise_validation_columns(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Add standard validation metadata columns."""

    validated_df = dataframe.copy()

    validated_df["rejection_reason"] = ""
    validated_df["validation_timestamp"] = datetime.now()

    return validated_df


def split_valid_invalid(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a validated dataframe into accepted and rejected records."""

    valid_records = dataframe[
        dataframe["rejection_reason"] == ""
    ].copy()

    invalid_records = dataframe[
        dataframe["rejection_reason"] != ""
    ].copy()

    valid_records = valid_records.drop(
        columns=["rejection_reason"]
    )

    return valid_records, invalid_records


def validate_required_columns(
    dataframe: pd.DataFrame,
    required_columns: list[str],
) -> list[str]:
    """Return required columns missing from the dataframe."""

    return [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]


def validate_null_values(
    dataframe: pd.DataFrame,
    required_columns: list[str],
) -> None:
    """Detect null or empty values in required columns."""

    for column in required_columns:
        condition = (
            dataframe[column].isna()
            | dataframe[column]
            .astype(str)
            .str.strip()
            .eq("")
        )

        append_rejection_reason(
            dataframe,
            condition,
            f"MISSING_REQUIRED_VALUE:{column}",
        )


def validate_duplicate_primary_keys(
    dataframe: pd.DataFrame,
    primary_key: str,
) -> None:
    """Detect duplicate primary-key values."""

    condition = dataframe.duplicated(
        subset=[primary_key],
        keep=False,
    )

    append_rejection_reason(
        dataframe,
        condition,
        f"DUPLICATE_PRIMARY_KEY:{primary_key}",
    )


def validate_allowed_values(
    dataframe: pd.DataFrame,
    column: str,
    allowed_values: set[str],
) -> None:
    """Validate that a column contains only allowed values."""

    condition = (
        dataframe[column].notna()
        & ~dataframe[column].isin(allowed_values)
    )

    append_rejection_reason(
        dataframe,
        condition,
        f"INVALID_VALUE:{column}",
    )


def validate_positive_number(
    dataframe: pd.DataFrame,
    column: str,
    allow_zero: bool = False,
) -> None:
    """Validate numeric columns for positive values."""

    numeric_values = pd.to_numeric(
        dataframe[column],
        errors="coerce",
    )

    if allow_zero:
        condition = numeric_values < 0
    else:
        condition = numeric_values <= 0

    condition = condition | numeric_values.isna()

    append_rejection_reason(
        dataframe,
        condition,
        f"INVALID_NUMERIC_VALUE:{column}",
    )


def validate_range(
    dataframe: pd.DataFrame,
    column: str,
    minimum: float,
    maximum: float,
) -> None:
    """Validate whether numeric values fall within a range."""

    numeric_values = pd.to_numeric(
        dataframe[column],
        errors="coerce",
    )

    condition = (
        numeric_values.isna()
        | (numeric_values < minimum)
        | (numeric_values > maximum)
    )

    append_rejection_reason(
        dataframe,
        condition,
        f"OUT_OF_RANGE:{column}",
    )


def validate_email(
    dataframe: pd.DataFrame,
    column: str,
) -> None:
    """Validate email address format."""

    condition = dataframe[column].apply(
        lambda value: (
            False
            if pd.isna(value)
            else EMAIL_PATTERN.match(str(value)) is None
        )
    )

    append_rejection_reason(
        dataframe,
        condition,
        f"INVALID_EMAIL:{column}",
    )


def validate_date_not_in_future(
    dataframe: pd.DataFrame,
    column: str,
) -> None:
    """Detect invalid dates and future dates."""

    converted_dates = pd.to_datetime(
        dataframe[column],
        errors="coerce",
    )

    current_timestamp = pd.Timestamp.now()

    condition = (
        converted_dates.isna()
        | (converted_dates > current_timestamp)
    )

    append_rejection_reason(
        dataframe,
        condition,
        f"INVALID_OR_FUTURE_DATE:{column}",
    )


def validate_foreign_key(
    dataframe: pd.DataFrame,
    foreign_key_column: str,
    valid_reference_values: set[str],
) -> None:
    """Validate references against a set of valid primary keys."""

    condition = (
        dataframe[foreign_key_column].notna()
        & ~dataframe[foreign_key_column].isin(
            valid_reference_values
        )
    )

    append_rejection_reason(
        dataframe,
        condition,
        f"INVALID_FOREIGN_KEY:{foreign_key_column}",
    )