import argparse

from src.analytics.runner import (
    run_analytics_pipeline,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build business analytics and KPI tables "
            "from the Gold dimensional model."
        )
    )

    parser.add_argument(
        "--batch-date",
        required=True,
        help="Batch date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    run_analytics_pipeline(
        batch_date=arguments.batch_date,
    )


if __name__ == "__main__":
    main()