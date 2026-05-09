"""Minimal stderr progress logger. Pipeline modules call info()/step()."""

from __future__ import annotations

import sys

_quiet = False


def set_quiet(quiet: bool) -> None:
    global _quiet
    _quiet = quiet


def info(msg: str) -> None:
    """One-line status update."""
    if not _quiet:
        print(msg, file=sys.stderr, flush=True)


def step(msg: str) -> None:
    """Section header for a pipeline stage."""
    if not _quiet:
        print(f"==> {msg}", file=sys.stderr, flush=True)
