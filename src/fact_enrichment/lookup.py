from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.spark_storage import (
    spark_path_exists,
)
from src.common.storage_config import (
    join_storage_path,
)


def read_parquet_dataset(
    spark: SparkSession,
    path: str,
    description: str,
) -> DataFrame:
    """
    Read a Parquet dataset from local storage
    or ADLS Gen2 and provide a clear error.
    """

    if not spark_path_exists(
        spark=spark,
        path=path,
    ):
        raise FileNotFoundError(
            f"{description} not found: {path}"
        )

    return spark.read.parquet(
        path
    )


def read_silver_fact(
    spark: SparkSession,
    silver_root: str,
    batch_date: str,
    dataset_name: str,
) -> DataFrame:
    """
    Read a Silver fact snapshot.
    """

    path = join_storage_path(
        silver_root,
        f"batch_date={batch_date}",
        dataset_name,
    )

    return read_parquet_dataset(
        spark=spark,
        path=path,
        description=f"Silver {dataset_name}",
    )


def read_scd2_dimension(
    spark: SparkSession,
    scd2_root: str,
    batch_date: str,
    dataset_name: str,
) -> DataFrame:
    """
    Read an SCD Type 2 dimension snapshot.
    """

    path = join_storage_path(
        scd2_root,
        f"batch_date={batch_date}",
        dataset_name,
    )

    return read_parquet_dataset(
        spark=spark,
        path=path,
        description=f"SCD2 {dataset_name}",
    )


def find_date_column(
    dataframe: DataFrame,
    candidates: list[str],
    dataset_name: str,
) -> str:
    """
    Find the first available event-date column.
    """

    for candidate in candidates:
        if candidate in dataframe.columns:
            return candidate

    raise ValueError(
        f"No supported date column was found in "
        f"{dataset_name}. Tried: {candidates}. "
        f"Available columns: {dataframe.columns}"
    )


def add_dimension_surrogate_key(
    dimension: DataFrame,
    business_keys: list[str],
    surrogate_key_name: str,
) -> DataFrame:
    """
    Create a deterministic surrogate key for each SCD2 version.

    The key is based on:
    - business key;
    - effective_from;
    - record_version.
    """

    key_columns = [
        F.coalesce(
            F.col(column_name).cast("string"),
            F.lit("NULL"),
        )
        for column_name in business_keys
    ]

    key_columns.extend(
        [
            F.coalesce(
                F.col("effective_from").cast("string"),
                F.lit("NULL"),
            ),
            F.coalesce(
                F.col("record_version").cast("string"),
                F.lit("NULL"),
            ),
        ]
    )

    return dimension.withColumn(
        surrogate_key_name,
        F.sha2(
            F.concat_ws("||", *key_columns),
            256,
        ),
    )


def add_order_surrogate_key(
    orders: DataFrame,
) -> DataFrame:
    """
    Create a stable order warehouse key.
    """

    return orders.withColumn(
        "order_sk",
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("ORDER"),
                F.coalesce(
                    F.col("order_id").cast("string"),
                    F.lit("NULL"),
                ),
            ),
            256,
        ),
    )


def historical_dimension_lookup(
    fact_dataframe: DataFrame,
    dimension_dataframe: DataFrame,
    fact_business_key: str,
    dimension_business_key: str,
    fact_date_column: str,
    surrogate_key_column: str,
    fact_primary_key: str,
) -> DataFrame:
    """
    Match each fact row to the dimension version valid
    on the fact event date.
    """

    fact_alias = "fact"
    dimension_alias = "dimension"

    fact = fact_dataframe.alias(fact_alias)
    dimension = dimension_dataframe.alias(
        dimension_alias
    )

    fact_date = F.to_date(
        F.col(f"{fact_alias}.{fact_date_column}")
    )

    join_condition = (
        F.col(
            f"{fact_alias}.{fact_business_key}"
        )
        ==
        F.col(
            f"{dimension_alias}.{dimension_business_key}"
        )
    ) & (
        fact_date
        >=
        F.col(
            f"{dimension_alias}.effective_from"
        )
    ) & (
        F.col(
            f"{dimension_alias}.effective_to"
        ).isNull()
        |
        (
            fact_date
            <=
            F.col(
                f"{dimension_alias}.effective_to"
            )
        )
    )

    joined = fact.join(
        dimension,
        on=join_condition,
        how="left",
    )

    match_window = (
        Window
        .partitionBy(
            F.col(
                f"{fact_alias}.{fact_primary_key}"
            )
        )
        .orderBy(
            F.col(
                f"{dimension_alias}.effective_from"
            ).desc_nulls_last(),
            F.col(
                f"{dimension_alias}.record_version"
            ).desc_nulls_last(),
        )
    )

    ranked = joined.withColumn(
        "_dimension_match_rank",
        F.row_number().over(match_window),
    )

    selected_fact_columns = [
        F.col(
            f"{fact_alias}.{column_name}"
        ).alias(column_name)
        for column_name in fact_dataframe.columns
    ]

    return (
        ranked
        .filter(
            F.col("_dimension_match_rank") == 1
        )
        .select(
            *selected_fact_columns,
            F.col(
                f"{dimension_alias}.{surrogate_key_column}"
            ).alias(surrogate_key_column),
        )
    )


def validate_no_missing_surrogate_keys(
    dataframe: DataFrame,
    surrogate_key_column: str,
    dataset_name: str,
) -> None:
    """
    Fail when any fact row cannot find its dimension.
    """

    missing_count = (
        dataframe
        .filter(
            F.col(surrogate_key_column).isNull()
        )
        .count()
    )

    if missing_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{missing_count} rows with no matching "
            f"{surrogate_key_column}."
        )


def validate_unique_fact_key(
    dataframe: DataFrame,
    primary_keys: list[str],
    dataset_name: str,
) -> None:
    """
    Ensure enrichment did not duplicate fact records.
    """

    duplicate_count = (
        dataframe
        .groupBy(*primary_keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{duplicate_count} duplicate fact keys "
            f"after enrichment."
        )


def write_enriched_fact(
    dataframe: DataFrame,
    output_root: str,
    batch_date: str,
    dataset_name: str,
) -> str:
    """
    Write an enriched fact dataset.
    """

    output_path = join_storage_path(
        output_root,
        f"batch_date={batch_date}",
        dataset_name,
    )

    (
        dataframe.write
        .mode("overwrite")
        .parquet(output_path)
    )

    return output_path


def print_missing_lookup_examples(
    dataframe: DataFrame,
    surrogate_key_column: str,
    selected_columns: list[str],
    dataset_name: str,
    limit: int = 10,
) -> None:
    """
    Display sample rows that failed dimension lookup.
    """

    missing_dataframe = dataframe.filter(
        F.col(surrogate_key_column).isNull()
    )

    missing_count = missing_dataframe.count()

    if missing_count == 0:
        return

    available_columns = [
        column_name
        for column_name in selected_columns
        if column_name in dataframe.columns
    ]

    print(
        f"\n{dataset_name}: {missing_count:,} rows "
        f"failed the {surrogate_key_column} lookup."
    )

    missing_dataframe.select(
        *available_columns
    ).show(
        limit,
        truncate=False,
    )