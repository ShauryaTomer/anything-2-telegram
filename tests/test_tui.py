import io

from rich.console import Console

from anything2telegram import tui


async def test_a_terminal_transfer_renders_a_live_bar(monkeypatch) -> None:
    output = io.StringIO()
    monkeypatch.setattr(
        tui, "CONSOLE", Console(file=output, force_terminal=True, width=120)
    )

    with tui.transfer("Upload", "clip.mp4", 100) as progress:
        progress(42, 100)

    rendered = output.getvalue()
    assert "clip.mp4" in rendered
    assert "42%" in rendered


async def test_a_headless_transfer_falls_back_to_log_lines(monkeypatch, caplog) -> None:
    monkeypatch.setattr(tui, "CONSOLE", Console(file=io.StringIO()))

    with caplog.at_level("INFO", logger="anything2telegram"):
        with tui.transfer("Upload", "clip.mp4", 100) as progress:
            progress(100, 100)

    assert "Upload progress: 100%" in caplog.text


async def test_an_unknown_total_is_reported_as_bytes(monkeypatch, caplog) -> None:
    monkeypatch.setattr(tui, "CONSOLE", Console(file=io.StringIO()))

    with caplog.at_level("INFO", logger="anything2telegram"):
        with tui.transfer("Download", "aaaaaaaaaaa", 0) as progress:
            progress(2_500_000, 0)

    assert "Download progress: 2.5 MB" in caplog.text
