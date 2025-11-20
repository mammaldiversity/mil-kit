# psd-toolkit

A Python toolkit for batch processing Adobe Photoshop PSD files. Automatically hide text layers and export to PNG format.

## Features

- 🚀 Batch process multiple PSD files in a directory
- 📝 Automatically hide all text layers
- 🖼️ Export processed files as PNG images
- 📁 Support for recursive directory processing
- ⚡ Preserve folder structure in output

## Installation

Install using pip:

```bash
pip install psd-toolkit
```

Or using uv:

```bash
uv add psd-toolkit
```

## Usage

### Command Line

Process PSD files in a directory:

```bash
psd-toolkit -d /path/to/psd/files
```

Process recursively and specify output directory:

```bash
psd-toolkit -d /path/to/psd/files -o /path/to/output -r
```

### Options

- `-d, --dir`: Input directory containing PSD files (required)
- `-o, --output`: Output directory for PNG files (default: input directory)
- `-r, --recursive`: Process subdirectories recursively

### Python API

You can also use psd-toolkit as a Python library:

```python
from psd_toolkit import PSDProcessor, BatchJob

# Process a single file
processor = PSDProcessor("image.psd")
processor.load()
processor.hide_text_layers()
processor.export_as_png("output.png")

# Batch process
job = BatchJob(input_dir="./psd_files", output_dir="./output", recursive=True)
job.run()
```

## Requirements

- Python >= 3.10
- psd-tools >= 1.12.0

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Author

Heru Handika

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Issues

Report bugs and request features on [GitHub Issues](https://github.com/hhandika/psd-toolkit/issues).
