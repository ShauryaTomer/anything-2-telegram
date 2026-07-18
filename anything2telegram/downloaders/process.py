import asyncio
import os
import signal
from collections.abc import Sequence

from ..domain import ProcessResult


class ProcessTimeoutError(TimeoutError):
    """The child exceeded its execution deadline."""


class YouTubeProcessRunner:
    def __init__(
        self,
        *,
        max_output_bytes: int = 1024 * 1024,
        shutdown_grace_seconds: float = 1,
    ) -> None:
        if type(max_output_bytes) is not int or max_output_bytes < 0:
            raise ValueError("max_output_bytes must be a nonnegative int")
        if (
            type(shutdown_grace_seconds) not in (int, float)
            or shutdown_grace_seconds < 0
        ):
            raise ValueError("shutdown_grace_seconds must be nonnegative")
        self._max_output_bytes = max_output_bytes
        self._shutdown_grace_seconds = shutdown_grace_seconds

    async def run(
        self, args: Sequence[str], timeout_seconds: float | int
    ) -> ProcessResult:
        if not isinstance(args, (list, tuple)) or not args:
            raise ValueError("args must be a nonempty argument vector")
        if not all(isinstance(arg, str) and arg for arg in args):
            raise ValueError("args must contain nonempty strings")
        if type(timeout_seconds) not in (int, float) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        stdout_task = asyncio.create_task(self._read_bounded(process.stdout))
        stderr_task = asyncio.create_task(self._read_bounded(process.stderr))
        wait_task = asyncio.create_task(process.wait())
        try:
            try:
                await asyncio.wait_for(asyncio.shield(wait_task), timeout_seconds)
            except TimeoutError:
                await self._terminate(process, wait_task)
                raise ProcessTimeoutError() from None
            stdout, stdout_count = await stdout_task
            _, stderr_count = await stderr_task
            return ProcessResult(
                process.returncode,
                stdout.decode("utf-8", errors="replace"),
                self._safe_stderr_summary(stderr_count),
            )
        except asyncio.CancelledError:
            await self._cleanup_after_interrupt(process, wait_task)
            raise
        except BaseException:
            await self._cleanup_after_interrupt(process, wait_task)
            raise
        finally:
            for task in (stdout_task, stderr_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)

    async def _read_bounded(
        self, stream: asyncio.StreamReader | None
    ) -> tuple[bytes, int]:
        if stream is None:
            return b"", 0
        stored = bytearray()
        total = 0
        while True:
            chunk = await stream.read(64 * 1024)
            if not chunk:
                return bytes(stored), total
            total += len(chunk)
            remaining = self._max_output_bytes - len(stored)
            if remaining > 0:
                stored.extend(chunk[:remaining])

    async def _cleanup_after_interrupt(self, process, wait_task: asyncio.Task) -> None:
        cleanup = asyncio.create_task(self._terminate(process, wait_task))
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup

    async def _terminate(self, process, wait_task: asyncio.Task) -> None:
        if process.returncode is None:
            self._signal_group(process.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(
                asyncio.shield(wait_task), self._shutdown_grace_seconds
            )
            return
        except TimeoutError:
            pass
        if process.returncode is None:
            self._signal_group(process.pid, signal.SIGKILL)
        await asyncio.shield(wait_task)

    @staticmethod
    def _signal_group(pid: int, sig: signal.Signals) -> None:
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            pass

    @staticmethod
    def _safe_stderr_summary(byte_count: int) -> str:
        if byte_count == 0:
            return ""
        return f"process stderr suppressed ({byte_count} bytes)"
