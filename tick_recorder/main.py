"""Entry point for the Tick-Recorder CLI.

Usage:
    python -m tick_recorder.main record config.json
    python -m tick_recorder.main inspect raw/demo/2026-10-29.bin --limit 100
"""

from __future__ import annotations

import logging
import sys 
from twisted.internet import task
import ctypes 

from tick_recorder.cli.parser import _build_parser
from tick_recorder.cli.commands import _run_record, _run_inspect

# Module-level logger — configured once on import.
logger = logging.getLogger("tick")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)

# ---------- MEMORY PROFILING SETUP ------------------------------------
def setup_memory_profiling(interval_minutes: int = 60):
    """Schedules periodic malloc_trim to prevent glibc arena fragmentation."""

    # Load glibc to call malloc_trim 
    try:
        libc = ctypes.CDLL("libc.so.6")
        # Explicitly define the return type for safety 
        libc.malloc_trim.restype = ctypes.c_int
        logger.info("[Memory] glibc loaded from malloc_trim.")
    except OSError:
        libc = None 
        logger.warning("[Memory] Could not load libc.so.6 for malloc_trim.")
    
    def run_malloc_trim():
        """Forces glibc to return freed memory to the OS."""
        if libc:
            freed = libc.malloc_trim(0)
            logger.info("[Memory] malloc_trim(0) called. Returned freed memory to OS.")

    # Run malloc_trim every hour to clear glibc arenas 
    lc = task.LoopingCall(run_malloc_trim)
    lc.start(interval_minutes * 60, now = False)
    logger.info("[Memory] Hourly malloc_trim scheduled (first at T+%d min)", interval_minutes)


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns process exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Apply --log-level to root tick logger.
    if hasattr(args, "log_level"):
        logger.setLevel(getattr(logging, args.log_level))

    if args.command == "record":
        # Initialize memory profiling ONLY for record command 
        setup_memory_profiling(interval_minutes=60)
        return _run_record(args)
    elif args.command == "inspect":
        return _run_inspect(args)
    else:
        # No subcommand — show help.
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
