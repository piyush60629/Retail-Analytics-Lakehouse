from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType


VALIDATION_METADATA_COLUMNS = [
    "_validation_errors",
    "_validation_status",
    "_validation_reason",
    "_validated_at",
]


def column_exists(
    dataframe: DataFrame,
    column_name: str,
) -> bool:
    return column_name in dataframe.columns


def trim_string_columns(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Trim leading and trailing whitespace from every string column.
    """

    string_columns = [
        field.name
        for field in dataframe.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    for column_name in string_columns:
        dataframe = dataframe.withColumn(
            column_name,
            F.trim(F.col(column_name)),
        )

    return dataframe


def lowercase_column(
    dataframe: DataFrame,
    column_name: str,
) -> DataFrame:
    if column_exists(dataframe, column_name):
        dataframe = dataframe.withColumn(
            column_name,
            F.lower(F.col(column_name)),
        )

    return dataframe


def uppercase_column(
    dataframe: DataFrame,
    column_name: str,
) -> DataFrame:
    if column_exists(dataframe, column_name):
        dataframe = dataframe.withColumn(
            column_name,
            F.upper(F.col(column_name)),
        )

    return dataframe


def titlecase_column(
    dataframe: DataFrame,
    column_name: str,
) -> DataFrame:
    if column_exists(dataframe, column_name):
        dataframe = dataframe.withColumn(
            column_name,
            F.initcap(
                F.lower(F.col(column_name))
            ),
        )

    return dataframe


def cast_integer_column(
    dataframe: DataFrame,
    column_name: str,
) -> DataFrame:
    if column_exists(dataframe, column_name):
        dataframe = dataframe.withColumn(
            column_name,
            F.col(column_name).cast("integer"),
        )

    return dataframe


def cast_decimal_column(
    dataframe: DataFrame,
    column_name: str,
    precision: int = 18,
    scale: int = 2,
) -> DataFrame:
    if column_exists(dataframe, column_name):
        dataframe = dataframe.withColumn(
            column_name,
            F.col(column_name).cast(
                DecimalType(precision, scale)
            ),
        )

    return dataframe


def cast_date_column(
    dataframe: DataFrame,
    column_name: str,
) -> DataFrame:
    if column_exists(dataframe, column_name):
        dataframe = dataframe.withColumn(
            column_name,
            F.to_date(F.col(column_name)),
        )

    return dataframe


def cast_timestamp_column(
    dataframe: DataFrame,
    column_name: str,
) -> DataFrame:
    if not column_exists(dataframe, column_name):
        return dataframe

    source_value = F.col(column_name)

    return dataframe.withColumn(
        column_name,
        F.coalesce(
            F.to_timestamp(
                source_value,
                "yyyy-MM-dd HH:mm:ss",
            ),
            F.to_timestamp(
                source_value,
                "yyyy-MM-dd HH:mm:ss.SSSSSS",
            ),
            F.to_timestamp(source_value),
        ),
    )


def remove_validation_metadata(
    dataframe: DataFrame,
) -> DataFrame:
    existing_columns = [
        column_name
        for column_name in VALIDATION_METADATA_COLUMNS
        if column_name in dataframe.columns
    ]

    if existing_columns:
        dataframe = dataframe.drop(*existing_columns)

    return dataframe


def add_silver_metadata(
    dataframe: DataFrame,
    batch_date: str,
) -> DataFrame:
    return (
        dataframe
        .withColumn(
            "_silver_batch_date",
            F.to_date(F.lit(batch_date)),
        )
        .withColumn(
            "_silver_processed_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_record_source",
            F.lit("BRONZE_VALIDATED"),
        )
    )


def finalize_silver_dataframe(
    dataframe: DataFrame,
    batch_date: str,
) -> DataFrame:
    """
    Apply common final Silver-layer operations.
    """

    dataframe = remove_validation_metadata(dataframe)

    return add_silver_metadata(
        dataframe=dataframe,
        batch_date=batch_date,
    )