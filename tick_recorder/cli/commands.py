"""CLI subcommand implementations."""

from __future__ import annotations

import os

from twisted.internet import reactor

from tick_recorder.recorder.inspect import inspect_raw
from tick_recorder.recorder.recorder import Recorder

def _run_record(args) -> int:
    """Subcommand: record."""

    # Recorder.__init__ does: config_path = os.path.join(base_path, config_name)
    # So if the user passed --base-path /var/lib/ftmo and config /var/lib/ftmo/conf.json,
    # we hand it just the basename and let Recorder resolve it under base_path.
    # If no --base-path, we hand the config path as-is and use "." as base.
    if args.base_path:
        config_name = os.path.basename(args.config)
        base_path = args.base_path
    else:
        config_name = args.config
        base_path = "."

    recorder = Recorder(config_name, base_path)
    recorder.start()
    reactor.run()
    return 0


def _run_inspect(args) -> int:
    """Subcommand: inspect."""
    # --limit 0 means "all"
    limit = None if args.limit == 0 else args.limit
    inspect_raw(args.file, limit)
    return 0
