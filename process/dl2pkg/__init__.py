"""dl2pkg: small package for dl2 components.

Modules:
- db: database helpers and schema
- das: DAS parsing helpers
- detector: detection and chunking logic
- downloader: download worker logic
- cli: thin CLI wiring
- utils: shared helpers

This package is intentionally small for refactor and testability.
"""

__all__ = ["db", "das", "detector", "downloader", "cli", "utils"]
