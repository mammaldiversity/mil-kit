from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm
from psd_toolkit.psd.processor import PSDProcessor


class BatchJob:
    """
    Manages the processing of a directory of files with parallel execution.
    """

    def __init__(
        self,
        input_dir,
        output_dir=None,
        recursive=False,
        output_format="png",
        max_workers=None,  # None = use default (number of CPUs)
    ):
        self.input_dir = Path(input_dir)
        self.recursive = recursive
        self.output_format = output_format.lower()
        self.max_workers = max_workers

        # If no output dir is specified, use input dir
        self.output_dir = (
            Path(output_dir) if output_dir else self.input_dir
        )

        self.stats = {"success": 0, "failed": 0, "skipped": 0}

    def run(self):
        """Executes the batch processing with parallel execution."""
        files = list(self._get_files())
        total_files = len(files)

        if total_files == 0:
            print(f"No PSD files found in {self.input_dir}")
            return

        print(
            f"Found {total_files} PSD file(s). Starting parallel processing...\n"
        )

        if total_files == 1:
            # For a single file, no need for parallelism
            success, message, count = self._process_single_file(
                files[0]
            )
            if success:
                self.stats["success"] += 1
            else:
                self.stats["failed"] += 1
            print(message)
            self._print_summary(total_files)
            return

        # Use ThreadPoolExecutor for I/O-bound tasks (file reading/writing)
        with ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            # Submit all tasks
            futures = {
                executor.submit(
                    self._process_single_file, psd_path
                ): psd_path
                for psd_path in files
            }

            with tqdm(
                total=total_files,
                desc="Processing PSD files",
                unit="file",
            ) as pbar:
                for future in as_completed(futures):
                    success, message, _ = future.result()

                    # Update stats
                    if success:
                        self.stats["success"] += 1
                    else:
                        self.stats["failed"] += 1

                    # Use tqdm.write() to avoid breaking the progress bar
                    tqdm.write(message)
                    pbar.update(1)

        self._print_summary(total_files)

    def _process_single_file(self, psd_path):
        """Process a single PSD file. Returns (success, message, count)."""
        try:
            processor = PSDProcessor(psd_path)
            processor.load()

            count = processor.hide_text_layers()

            dest_path = self._generate_output_path(psd_path)
            processor.export(dest_path, format=self.output_format)

            return (
                True,
                f"✓ {psd_path.name}: Hidden {count} layers",
                count,
            )

        except Exception as e:
            return (False, f"✗ {psd_path.name}: Failed - {e}", 0)

    def _get_files(self):
        """Generator that yields PSD files."""
        pattern = "**/*.psd" if self.recursive else "*.psd"
        return self.input_dir.glob(pattern)

    def _generate_output_path(self, psd_path):
        """Generate the output file path."""
        return (
            self.output_dir / f"{psd_path.stem}.{self.output_format}"
        )

    def _print_summary(self, total_files):
        """Print processing summary."""
        print("\n" + "=" * 50)
        print("Processing complete!")
        print(f"Total: {total_files}")
        print(f"Success: {self.stats['success']}")
        print(f"Failed: {self.stats['failed']}")
        print("=" * 50)
