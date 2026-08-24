# s3-benchmark

Benchmark S3 upload and download throughput across different object sizes, and
tune `boto3` `TransferConfig` parameters (multipart threshold, concurrency,
chunk size) for your endpoint.

Works with AWS S3 and any S3-compatible object store (the `.env-example` is
configured for Civo's object storage, but any endpoint works).

## Features

- **Upload benchmark** — measure throughput (Mbps) for a list of file sizes.
- **Download benchmark** — measure download throughput for a list of file sizes.
- **Tuner** — grid-search `TransferConfig` parameters and report the fastest
  combination (download based).
- CSV output + PNG plots for every run.
- Streaming file generation (won't run out of RAM on multi-GB objects).
- Validation of `TransferConfig` values against S3 limits.

## Installation

```bash
git clone https://github.com/jmesout/s3-benchmark.git
cd s3-benchmark
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

## Configuration

Copy the example environment file and fill in your credentials:

```bash
cp .env-example .env
```

Required variables:

| Variable | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | Access key (optional if using the default credential chain) |
| `AWS_SECRET_ACCESS_KEY` | Secret key (optional if using the default chain) |
| `S3_ENDPOINT_URL` | Endpoint URL (omit for AWS) |
| `S3_BUCKET_NAME` | Bucket to benchmark against |

Optional variables (with defaults):

| Variable | Default | Description |
|---|---|---|
| `FILE_SIZES` | `100,500,1024,5120,10240,20480,51200,102400` | Comma-separated MB sizes |
| `MULTIPART_THRESHOLD` | `52428800` | Threshold (bytes) to switch to multipart |
| `MAX_CONCURRENCY` | `10` | Concurrent threads for multipart transfers |
| `MULTIPART_CHUNKSIZE` | `52428800` | Part size (bytes); must be 5 MiB–5 GiB |
| `USE_THREADS` | `True` | Use threads for multipart transfers |
| `SIGNATURE_VERSION` | `s3v4` | Sig version (`s3v4` or `s3`) |
| `TUNE_*` | — | Ranges for the tuner (see `.env-example`) |

## Usage

```bash
# Upload throughput benchmark
s3-benchmark upload

# Download throughput benchmark
s3-benchmark download

# Grid-search TransferConfig parameters
s3-benchmark tune
```

Or without installing:

```bash
python -m s3benchmark upload
```

Results are written as timestamped files in the current directory:
`upload_results_*.csv`, `upload_speeds_*.png`, `download_results_*.csv`,
`download_speeds_*.png`, `tuning_results_*.csv`, `tuning_plot_*.png`, and
`fastest_configuration_*.json`.

## How it works

1. **Upload/download** — for each size in `FILE_SIZES`, a dummy file is
   generated (streamed in 8 MiB chunks), transferred with `upload_file` /
   `download_file`, timed with a monotonic clock, then removed.
2. **Tuner** — downloads a single object using every combination of the
   `TUNE_*` ranges, records throughput, and reports the fastest config.

## Development

```bash
pip install -e ".[dev]"
pytest
```

## Notes

- The download benchmark assumes objects named `example_<size>mb.txt` already
  exist in the bucket (run the upload benchmark first).
- The tuner assumes `example_<TUNE_FILE_SIZE>mb.txt` exists.