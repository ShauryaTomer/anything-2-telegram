"""Terminal progress for transfers: a live bar on a TTY, log lines otherwise."""

import asyncio
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from .jobs.progress import ProgressWriter


_LOGGER = logging.getLogger(__name__)
_LOG_INTERVAL_SECONDS = 1.0

CONSOLE = Console(stderr=True)

_NAME_WIDTH = 28
_COLUMNS = (
    TextColumn("[bold blue]{task.description}"),
    # bar_width=None lets the bar absorb whatever width is left, so an 80-column
    # terminal shows the same columns without wrapping them onto a second line.
    BarColumn(bar_width=None),
    TaskProgressColumn(),
    DownloadColumn(),
    TransferSpeedColumn(),
    TimeRemainingColumn(),
)


@contextmanager
def transfer(
    label: str,
    name: str,
    total: int,
    progress: ProgressWriter | None = None,
) -> Iterator[Callable[[int, int], None]]:
    """Yield a `(sent, total)` callback that reports progress for one transfer.

    On a TTY the callback drives a live rich bar that disappears when the
    transfer ends. Headless (systemd, docker, tests) it falls back to the
    throttled log line, which is the only progress an operator ever sees there.
    An optional registry writer records the same numbers regardless of which
    branch renders them, so the web UI stays live either way.
    """
    cm = (
        nullcontext(_log_reporter(label))
        if not CONSOLE.is_terminal
        else _rich_reporter(label, name, total)
    )
    with cm as inner:
        def report(sent: int, total_bytes: int) -> None:
            inner(sent, total_bytes)
            if progress is not None:
                progress.update(sent, total_bytes)

        try:
            if progress is not None:
                progress.update(0, total)
            yield report
        finally:
            if progress is not None:
                progress.done()


@contextmanager
def _rich_reporter(label: str, name: str, total: int) -> Iterator[Callable[[int, int], None]]:
    # ponytail: a fresh Progress per transfer, because the scheduler runs one
    # transfer at a time. Hoist it to a module-level Live if that ever changes.
    with Progress(*_COLUMNS, console=CONSOLE, transient=True) as rich_progress:
        task = rich_progress.add_task(
            f"{label} {name[:_NAME_WIDTH]}", total=total or None
        )

        def report(sent: int, total_bytes: int) -> None:
            rich_progress.update(task, completed=sent, total=total_bytes or None)

        yield report


def _log_reporter(label: str) -> Callable[[int, int], None]:
    last_reported = 0.0

    def report(sent: int, total: int) -> None:
        nonlocal last_reported
        now = asyncio.get_running_loop().time()
        complete = total > 0 and sent >= total
        if not complete and now - last_reported < _LOG_INTERVAL_SECONDS:
            return
        last_reported = now
        # An unknown total (yt-dlp writes files whose final size nobody knows
        # yet) still has a byte count worth printing.
        done = f"{sent * 100 // total}%" if total else format_bytes(sent)
        _LOGGER.info("%s progress: %s", label, done)

    return report


def format_bytes(sent: int) -> str:
    """Render a byte count the way the headless log line does, e.g. '2.5 MB'."""
    return f"{sent / 1e6:.1f} MB"


def install_log_handler(level: int) -> bool:
    """Route logs through rich so records print above a live bar, not into it.

    Returns False when stderr is not a terminal, leaving log configuration to
    the caller.
    """
    if not CONSOLE.is_terminal:
        return False
    logging.basicConfig(
        format="%(message)s",
        datefmt="%H:%M:%S",
        level=level,
        handlers=[RichHandler(console=CONSOLE, rich_tracebacks=True)],
    )
    return True
