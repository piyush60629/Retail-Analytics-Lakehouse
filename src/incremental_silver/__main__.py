import argparse

from src.incremental_silver.runner import (
    run_incremental_silver,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Merge CDC changes into a new Silver snapshot."
        )
    )

    parser.add_argument(
        "--batch-date",
        required=True,
        help="CDC batch date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    run_incremental_silver(
        batch_date=arguments.batch_date,
    )


if __name__ == "__main__":
    main()