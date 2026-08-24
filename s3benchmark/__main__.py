"""Allow running the package directly: ``python -m s3benchmark``."""
import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
