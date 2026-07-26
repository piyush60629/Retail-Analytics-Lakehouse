import argparse

from src.scd2.runner import run_scd2_pipeline


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Initialise or update SCD Type 2 dimensions."
        )
    )

    parser.add_argument(
        "--batch-date",
        required=True,
        help="Current batch date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--initialise",
        action="store_true",
        help=(
            "Initialise dimensions from the Silver "
            "batch supplied through --batch-date."
        ),
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    if arguments.initialise:
        run_scd2_pipeline(
            batch_date=arguments.batch_date,
            initial_batch_date=arguments.batch_date,
        )
    else:
        run_scd2_pipeline(
            batch_date=arguments.batch_date,
        )


if __name__ == "__main__":
    main()