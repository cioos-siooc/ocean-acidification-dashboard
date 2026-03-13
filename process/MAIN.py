#!/usr/bin/env python3
"""Legacy entrypoint wrapper for the dl2 package.

This module provides a small, stable CLI entrypoint that delegates to
`modules.cli`. Keep this file minimal to avoid duplicating logic.
"""

from modules.cli import main as package_main


def main(argv=None):
    package_main(argv)


if __name__ == "__main__":
    main()
