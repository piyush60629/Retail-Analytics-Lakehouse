import argparse

from src.incremental.record_cdc import (
    run_record_level_cdc,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare the current Bronze batch with "
            "the previous Bronze batch."
        )
    )

    parser.add_argument(
        "--batch-date",
        required=True,
        help="Current Bronze batch date.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    run_record_level_cdc(
        current_batch_date=arguments.batch_date,
    )


if __name__ == "__main__":
    main()