import asyncio
import errno
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
        process_group_id = process.pid
        try:
            _, pending = await asyncio.wait(
                (wait_task, stdout_task, stderr_task),
                timeout=timeout_seconds,
            )
            if pending:
                await self._terminate(process_group_id, wait_task)
                raise ProcessTimeoutError() from None
            stdout, stdout_count = await stdout_task
            _, stderr_count = await stderr_task
            return ProcessResult(
                process.returncode,
                stdout.decode("utf-8", errors="replace"),
                self._safe_stderr_summary(stderr_count),
            )
        except asyncio.CancelledError:
            await self._cleanup_after_interrupt(process_group_id, wait_task)
            raise
        except ProcessTimeoutError:
            raise
        except BaseException:
            await self._cleanup_after_interrupt(process_group_id, wait_task)
            raise
        finally:
            for task in (stdout_task, stderr_task):
                if not task.done():
                    task.cancel()
            done, _ = await asyncio.wait(
                (stdout_task, stderr_task),
                timeout=max(self._shutdown_grace_seconds, 0.1),
            )
            for task in done:
                try:
                    task.exception()
                except asyncio.CancelledError:
                    pass

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

    async def _cleanup_after_interrupt(
        self, process_group_id: int, wait_task: asyncio.Task
    ) -> None:
        cleanup = asyncio.create_task(
            self._terminate(process_group_id, wait_task)
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup

    async def _terminate(
        self, process_group_id: int, wait_task: asyncio.Task
    ) -> None:
        if not self._signal_group(process_group_id, signal.SIGTERM):
            await asyncio.shield(wait_task)
            return
        if await self._group_survives_grace(process_group_id):
            self._signal_group(process_group_id, signal.SIGKILL)
        await asyncio.shield(wait_task)

    @staticmethod
    def _signal_group(pid: int, sig: signal.Signals) -> bool:
        try:
            os.killpg(pid, sig)
            return True
        except OSError as error:
            if (
                isinstance(error, ProcessLookupError)
                or error.errno == errno.ESRCH
            ):
                return False
            raise

    async def _group_survives_grace(self, process_group_id: int) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._shutdown_grace_seconds
        while True:
            if not self._group_exists(process_group_id):
                return False
            remaining = deadline - loop.time()
            if remaining <= 0:
                return True
            await asyncio.sleep(min(0.05, remaining))

    @staticmethod
    def _group_exists(process_group_id: int) -> bool:
        try:
            os.killpg(process_group_id, 0)
            return True
        except OSError as error:
            if (
                isinstance(error, ProcessLookupError)
                or error.errno == errno.ESRCH
            ):
                return False
            if error.errno == errno.EPERM:
                return True
            raise

    @staticmethod
    def _safe_stderr_summary(byte_count: int) -> str:
        if byte_count == 0:
            return ""
        return f"process stderr suppressed ({byte_count} bytes)"
