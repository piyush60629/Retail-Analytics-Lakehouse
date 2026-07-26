import argparse

from src.silver.runner import run_silver_pipeline


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Transform validated Bronze records "
            "into Silver datasets."
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

    run_silver_pipeline(
        batch_date=arguments.batch_date,
    )


if __name__ == "__main__":
    main()