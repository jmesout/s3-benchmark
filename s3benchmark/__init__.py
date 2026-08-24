"""s3benchmark — S3 upload/download throughput benchmarking for boto3.

A small, dependency-light toolkit for measuring S3 transfer throughput
across different object sizes and ``boto3`` ``TransferConfig`` settings.
"""

__version__ = "1.0.0"

from .config import TransferParams, load_config, parse_bool
from .io import calculate_speed, create_dummy_file
from .report import plot_results, save_results_to_csv
from .transfer import create_s3_client

__all__ = [
    "load_config",
    "TransferParams",
    "create_s3_client",
    "create_dummy_file",
    "calculate_speed",
    "parse_bool",
    "save_results_to_csv",
    "plot_results",
]
