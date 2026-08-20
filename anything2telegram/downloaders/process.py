import asyncio
import errno
import os
import signal
from collections.abc import Sequence

from ..domain import ProcessResult


_MAX_OUTPUT_BYTES = 1024 * 1024
_SHUTDOWN_GRACE_SECONDS = 1.0
_STDERR_TAIL_CHARS = 2000


class ProcessTimeoutError(TimeoutError):
    """The child exceeded its execution deadline."""


class YouTubeProcessRunner:
    """Runs yt-dlp in its own process group so a timeout can kill the tree."""

    async def run(
        self, args: Sequence[str], timeout_seconds: float
    ) -> ProcessResult:
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
            stdout, _ = await stdout_task
            stderr, _ = await stderr_task
            return ProcessResult(
                process.returncode,
                stdout.decode("utf-8", errors="replace"),
                self._stderr_tail(stderr),
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
                timeout=max(_SHUTDOWN_GRACE_SECONDS, 0.1),
            )
            for task in done:
                try:
                    task.exception()
                except asyncio.CancelledError:
                    pass

    @staticmethod
    async def _read_bounded(
        stream: asyncio.StreamReader | None
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
            remaining = _MAX_OUTPUT_BYTES - len(stored)
            if remaining > 0:
                stored.extend(chunk[:remaining])

    @classmethod
    async def _cleanup_after_interrupt(
        cls, process_group_id: int, wait_task: asyncio.Task
    ) -> None:
        cleanup = asyncio.create_task(
            cls._terminate(process_group_id, wait_task)
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup

    @classmethod
    async def _terminate(
        cls, process_group_id: int, wait_task: asyncio.Task
    ) -> None:
        if not cls._signal_group(process_group_id, signal.SIGTERM):
            await asyncio.shield(wait_task)
            return
        if await cls._group_survives_grace(process_group_id):
            cls._signal_group(process_group_id, signal.SIGKILL)
        await asyncio.shield(wait_task)

    @staticmethod
    def _signal_group(pid: int, sig: signal.Signals) -> bool:
        try:
            os.killpg(pid, sig)
            return True
        except OSError as error:
            if isinstance(error, ProcessLookupError) or error.errno == errno.ESRCH:
                return False
            raise

    @classmethod
    async def _group_survives_grace(cls, process_group_id: int) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _SHUTDOWN_GRACE_SECONDS
        while True:
            if not cls._group_exists(process_group_id):
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
            if isinstance(error, ProcessLookupError) or error.errno == errno.ESRCH:
                return False
            if error.errno == errno.EPERM:
                return True
            raise

    @staticmethod
    def _stderr_tail(raw: bytes) -> str:
        """The end of stderr, where yt-dlp puts the reason it gave up."""
        # ponytail: _read_bounded keeps the first _MAX_OUTPUT_BYTES, so on
        # stderr larger than that this is the tail of the head. Fine for
        # yt-dlp; revisit if a runner ever produces megabytes of stderr.
        return raw.decode("utf-8", errors="replace").strip()[-_STDERR_TAIL_CHARS:]
