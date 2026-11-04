"""argparse setup for the Tick-Recorder CLI."""

from __future__ import annotations

import argparse
import os


def _existing_file(path: str) -> str:
    """argparse type= callback: file must exist or argparse exits with code 2."""
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"file not found: {path}")
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"not a regular file: {path}")
    return path


def _positive_int(value: str) -> int:
    """argparse type= callback: must be a positive integer."""
    try:
        n = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an integer: {value!r}")
    if n < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {n}")
    return n


def _build_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser. Kept in a function so tests can call it."""
    parser = argparse.ArgumentParser(
        prog="tick_recorder",
        description="cTrader live tick recorder — record raw price feed "
                    "and inspect recorded .bin files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Start recording from a config\n"
            "  python -m tick_recorder.main record recorder_config.json\n\n"
            "  # Inspect first 100 frames of a recorded .bin file\n"
            "  python -m tick_recorder.main inspect raw/demo/2026-10-29.bin --limit 100\n"
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity (default: INFO).",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="<command>")

    # ---- record ----
    p_rec = subparsers.add_parser(
        "record",
        help="Start the recorder with a config file (Ctrl+C to stop).",
    )
    p_rec.add_argument(
        "config",
        type=_existing_file,
        help="Path to the recorder JSON config file.",
    )
    p_rec.add_argument(
        "--base-path",
        default="",
        help="Base directory for data_dir and config file resolution "
             "(default: current working directory).",
    )

    # ---- inspect ----
    p_ins = subparsers.add_parser(
        "inspect",
        help="Decode and print frames from a recorded .bin file.",
    )
    p_ins.add_argument(
        "file",
        type=_existing_file,
        help="Path to the raw .bin file to inspect.",
    )
    p_ins.add_argument(
        "--limit",
        type=_positive_int,
        default=50,
        help="Maximum number of frames to print (default: 50). "
             "Pass --limit 0 to print ALL frames (use with caution).",
    )

    return parser
