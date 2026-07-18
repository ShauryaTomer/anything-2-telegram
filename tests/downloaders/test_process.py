import asyncio
import os
import signal

import pytest

from anything2telegram.domain import ProcessResult as DomainProcessResult
from anything2telegram.downloaders.process import (
    ProcessResult,
    ProcessTimeoutError,
    YouTubeProcessRunner,
)


def test_process_adapter_uses_shared_domain_result() -> None:
    assert ProcessResult is DomainProcessResult
    assert ProcessResult(0, "", "").exit_code == 0


class FakeProcess:
    def __init__(self, *, returncode: int | None = 0, block: bool = False) -> None:
        self.pid = 4321
        self.returncode = returncode
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self._done = asyncio.Event()
        if not block:
            self._done.set()

    async def wait(self) -> int:
        await self._done.wait()
        if self.returncode is None:
            self.returncode = -signal.SIGTERM
        return self.returncode

    def finish(self, returncode: int = 0) -> None:
        self.returncode = returncode
        self.stdout.feed_eof()
        self.stderr.feed_eof()
        self._done.set()


@pytest.mark.asyncio
async def test_runner_uses_argument_vector_new_session_and_bounds_output(monkeypatch) -> None:
    process = FakeProcess()
    process.stdout.feed_data(b"abcdef")
    process.stdout.feed_eof()
    process.stderr.feed_data(b"secret URL cookie path")
    process.stderr.feed_eof()
    spawned = []

    async def create(*args, **kwargs):
        spawned.append((args, kwargs))
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    result = await YouTubeProcessRunner(max_output_bytes=4).run(
        ["yt-dlp", "--version"], 1
    )

    assert spawned[0][0] == ("yt-dlp", "--version")
    assert spawned[0][1]["start_new_session"] is True
    assert result == ProcessResult(0, "abcd", "process stderr suppressed (22 bytes)")
    assert "secret" not in result.stderr_safe_summary


@pytest.mark.asyncio
async def test_timeout_terminates_group_then_kills_and_reaps(monkeypatch) -> None:
    process = FakeProcess(returncode=None, block=True)
    calls = []

    async def create(*args, **kwargs):
        return process

    def killpg(pid, sig):
        calls.append((pid, sig))
        if sig == signal.SIGKILL:
            process.finish(-signal.SIGKILL)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(os, "killpg", killpg)

    with pytest.raises(ProcessTimeoutError):
        await YouTubeProcessRunner(shutdown_grace_seconds=0).run(["yt-dlp"], 0.001)

    assert [call for call in calls if call[1] != 0] == [
        (4321, signal.SIGTERM),
        (4321, signal.SIGKILL),
    ]
    assert process.returncode == -signal.SIGKILL


@pytest.mark.asyncio
async def test_cancellation_cleans_process_group_and_reraises(monkeypatch) -> None:
    process = FakeProcess(returncode=None, block=True)
    calls = []

    async def create(*args, **kwargs):
        return process

    def killpg(pid, sig):
        calls.append(sig)
        if sig == signal.SIGTERM:
            process.finish(-sig)
        elif sig == 0:
            raise ProcessLookupError

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(os, "killpg", killpg)
    task = asyncio.create_task(YouTubeProcessRunner().run(["yt-dlp"], 10))
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls == [signal.SIGTERM, 0]
    assert process.returncode is not None


@pytest.mark.asyncio
async def test_spawn_error_propagates_without_signaling(monkeypatch) -> None:
    async def create(*args, **kwargs):
        raise FileNotFoundError("yt-dlp")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    with pytest.raises(FileNotFoundError):
        await YouTubeProcessRunner().run(["yt-dlp"], 1)


@pytest.mark.asyncio
async def test_exit_during_timeout_signal_is_race_safe(monkeypatch) -> None:
    process = FakeProcess(returncode=None, block=True)

    async def create(*args, **kwargs):
        return process

    def killpg(pid, sig):
        process.finish(0)
        raise ProcessLookupError

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(os, "killpg", killpg)
    with pytest.raises(ProcessTimeoutError):
        await YouTubeProcessRunner().run(["yt-dlp"], 0.001)


@pytest.mark.asyncio
async def test_timeout_kills_group_when_leader_exits_but_descendant_holds_pipes(
    monkeypatch,
) -> None:
    process = FakeProcess(returncode=None, block=True)
    calls = []

    async def create(*args, **kwargs):
        return process

    def killpg(pid, sig):
        calls.append(sig)
        if sig == signal.SIGTERM:
            process.returncode = 0
            process._done.set()
        elif sig == signal.SIGKILL:
            process.stdout.feed_eof()
            process.stderr.feed_eof()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(os, "killpg", killpg)

    with pytest.raises(ProcessTimeoutError):
        await asyncio.wait_for(
            YouTubeProcessRunner(shutdown_grace_seconds=0.001).run(
                ["yt-dlp"], 0.001
            ),
            0.1,
        )
    assert calls[0] == signal.SIGTERM
    assert 0 in calls
    assert calls[-1] == signal.SIGKILL
