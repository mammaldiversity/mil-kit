import argparse
import sys
from psd_toolkit.job import BatchJob


def main():
    parser = argparse.ArgumentParser(
        description="Batch hide text layers in PSDs and export PNGs."
    )
    parser.add_argument(
        "-d",
        "--dir",
        required=True,
        help="Input directory containing PSD files",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output directory (default: input directory)",
    )
    parser.add_argument(
        "-f",
        "--output-format",
        default="png",
        help="Output format (default: png)",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Process subdirectories recursively",
    )

    args = parser.parse_args()

    try:
        job = BatchJob(args.dir, args.output, args.recursive)
        job.run()
    except Exception as e:
        print(f"Critical Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
