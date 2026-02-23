import argparse
import logging
from pathlib import Path

from src.build import full_build, incremental_build

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def main():
    parser = argparse.ArgumentParser(
        description="Build Gutenberg HuggingFace dataset"
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Run full build (downloads everything, ~10GB)",
    )
    parser.add_argument(
        "--incremental", action="store_true",
        help="Run incremental update (only new books)",
    )
    parser.add_argument(
        "--repo-id", required=True,
        help="HuggingFace repo ID (e.g., user/gutenberg-corpus)",
    )
    parser.add_argument(
        "--data-dir", default="./data",
        help="Local data directory (default: ./data)",
    )
    parser.add_argument(
        "--no-dedup", action="store_true",
        help="Skip deduplication (include all versions of duplicate books)",
    )

    args = parser.parse_args()
    data_dir = Path(args.data_dir)

    if args.full:
        full_build(args.repo_id, data_dir, dedup=not args.no_dedup)
    elif args.incremental:
        incremental_build(args.repo_id, data_dir, dedup=not args.no_dedup)
    else:
        parser.error("Specify --full or --incremental")


if __name__ == "__main__":
    main()
