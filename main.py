import argparse
import sys

sys.dont_write_bytecode = True

from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_a, **_kw):  # pragma: no cover
        return False

load_dotenv(Path(__file__).resolve().parent / ".env")


def main() -> None:
    parser = argparse.ArgumentParser(description="CropGuard pipelines")
    parser.add_argument(
        "--forecast-only",
        action="store_true",
        help="Run only the forecast pipeline (drift check, optional retrain, infer, publish).",
    )
    args = parser.parse_args()

    if args.forecast_only:
        from orchestration.forecast_flow import cropguard_forecast_pipeline

        result = cropguard_forecast_pipeline()
        print(result)
        return

    from orchestration.flow import cropguard_bronze_pipeline

    result = cropguard_bronze_pipeline()
    print(result)


if __name__ == "__main__":
    main()
