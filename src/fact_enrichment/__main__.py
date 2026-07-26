import argparse

from src.fact_enrichment.runner import (
    run_fact_enrichment,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Add dimension surrogate keys to Silver facts."
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

    run_fact_enrichment(
        batch_date=arguments.batch_date,
    )


if __name__ == "__main__":
    main()