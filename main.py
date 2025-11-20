import argparse
import sys
from pathlib import Path
from psd_tools import PSDImage


class PSDProcessor:
    """
    Handles the loading, modification, and exporting of a single PSD file.
    """

    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.psd = None
        self.hidden_count = 0

    def load(self):
        """Loads the PSD file."""
        try:
            self.psd = PSDImage.open(self.file_path)
        except Exception as e:
            raise IOError(f"Failed to open PSD: {e}")

    def hide_text_layers(self):
        """Iterates through all layers and hides those of kind 'type'."""
        if not self.psd:
            raise RuntimeError("PSD not loaded. Call load() first.")

        self.hidden_count = 0
        # descendants() iterates recursively through groups
        for layer in self.psd.descendants():
            if layer.kind == "type" and layer.visible:
                layer.visible = False
                self.hidden_count += 1

        return self.hidden_count

    def export_as_png(self, output_path):
        """Composites the PSD and saves as PNG."""
        if not self.psd:
            raise RuntimeError("PSD not loaded.")

        # Ensure the target directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        # Composite merges layers; save exports using PIL/Pillow
        self.psd.composite().save(output_path)


class BatchJob:
    """
    Manages the processing of a directory of files.
    """

    def __init__(self, input_dir, output_dir=None, recursive=False):
        self.input_dir = Path(input_dir)
        self.recursive = recursive

        # If no output dir is specified, use input dir
        self.output_dir = (
            Path(output_dir) if output_dir else self.input_dir
        )

        self.stats = {"success": 0, "failed": 0, "skipped": 0}

    def _get_files(self):
        """Generator that yields PSD files based on recursion setting."""
        if not self.input_dir.exists():
            raise FileNotFoundError(
                f"Input directory not found: {self.input_dir}"
            )

        pattern = "*.psd"
        return (
            self.input_dir.rglob(pattern)
            if self.recursive
            else self.input_dir.glob(pattern)
        )

    def _calculate_output_path(self, psd_path):
        """Determines the destination path, preserving folder structure if recursive."""
        if self.recursive and self.output_dir != self.input_dir:
            # Create mirror structure: output/subfolder/file.png
            rel_path = psd_path.relative_to(self.input_dir)
            return self.output_dir / rel_path.with_suffix(".png")
        else:
            # Flat structure or same folder
            return self.output_dir / f"{psd_path.stem}.png"

    def run(self):
        """Executes the batch processing."""
        files = list(self._get_files())
        total_files = len(files)

        if total_files == 0:
            print(f"No PSD files found in {self.input_dir}")
            return

        print(
            f"Found {total_files} PSD file(s). Starting process...\n"
        )

        for i, psd_path in enumerate(files, 1):
            print(
                f"[{i}/{total_files}] Processing: {psd_path.name}...",
                end=" ",
                flush=True,
            )

            try:
                processor = PSDProcessor(psd_path)
                processor.load()

                count = processor.hide_text_layers()

                dest_path = self._calculate_output_path(psd_path)
                processor.export_as_png(dest_path)

                print(f"Done. (Hidden {count} layers)")
                self.stats["success"] += 1

            except Exception as e:
                print(f"\n    X Failed: {e}")
                self.stats["failed"] += 1

        self._print_summary(total_files)

    def _print_summary(self, total):
        print(f"\n{'=' * 40}")
        print("Processing Complete")
        print(f"Total Files: {total}")
        print(f"Successful:  {self.stats['success']}")
        print(f"Failed:      {self.stats['failed']}")
        print(f"{'=' * 40}")


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
