import argparse

from src.gold.runner import run_gold_pipeline


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create Gold dimensional tables "
            "from Silver datasets."
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

    run_gold_pipeline(
        batch_date=arguments.batch_date,
    )


if __name__ == "__main__":
    main()