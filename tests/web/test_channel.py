from types import SimpleNamespace

from tests.web.conftest import _client, _drain

CHANNEL = "https://www.youtube.com/@Java.Brains"
TAB = "https://www.youtube.com/@Java.Brains/playlists"
SPRING = "https://www.youtube.com/playlist?list=PLspring"
DOCKER = "https://www.youtube.com/playlist?list=PLdocker"


def _two_playlists(app) -> None:
    runner = app.state.channel_runner
    runner.playlists = [
        {
            "id": "PLspring",
            "title": "Spring Boot",
            "thumbnails": [
                {"url": "https://i.ytimg.com/vi/aaa/default.jpg", "width": 120},
                {"url": "https://i.ytimg.com/vi/aaa/hqdefault.jpg", "width": 480},
            ],
        },
        {"id": "PLdocker", "title": "Docker"},
    ]
    runner.counts = {SPRING: 3, DOCKER: 1}


async def test_channel_page_offers_a_fetch_form_and_a_way_back(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.get("/channel")

    assert response.status_code == 200
    body = response.text
    assert 'hx-post="/web/channel"' in body
    assert 'name="url"' in body
    assert '<a href="/">' in body


async def test_the_main_page_links_to_the_channel_page(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.get("/")

    assert '<a href="/channel">' in response.text


async def test_fetching_a_channel_lists_its_playlists_with_titles_and_counts(
    wired_app,
) -> None:
    _two_playlists(wired_app)

    async with await _client(wired_app) as client:
        response = await client.post("/web/channel", data={"url": CHANNEL})

    assert response.status_code == 200
    body = response.text
    assert "Spring Boot" in body
    assert "3 videos" in body
    assert "Docker" in body
    assert "1 video<" in body
    # The widest thumbnail the listing offered; the untumbnailed one gets a box.
    assert '<img src="https://i.ytimg.com/vi/aaa/hqdefault.jpg"' in body
    assert '<span class="thumb-missing">' in body
    assert f'value="{SPRING}"' in body
    assert f'value="{DOCKER}"' in body
    assert "Select all" in body
    # The channel's own /playlists tab, then one count call per playlist.
    assert wired_app.state.channel_runner.calls == [TAB, SPRING, DOCKER]


async def test_a_playlist_whose_title_is_missing_falls_back_to_its_id(
    wired_app,
) -> None:
    wired_app.state.channel_runner.playlists = [{"id": "PLspring"}, "not-a-dict"]

    async with await _client(wired_app) as client:
        response = await client.post("/web/channel", data={"url": CHANNEL})

    body = response.text
    assert "PLspring" in body
    assert body.count('name="playlist"') == 1


async def test_selected_playlists_each_become_a_batch_in_the_queue(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/channel/queue", data={"playlist": [SPRING, DOCKER]}
        )
        assert response.status_code == 303
        assert response.headers["location"] == "/"

        await _drain(wired_app.state.bus)
        queue = await client.get("/web/queue")

    assert SPRING in queue.text
    assert DOCKER in queue.text
    assert wired_app.state.scheduler.calls == [
        ("playlist", (SPRING, 0)),
        ("playlist", (DOCKER, 0)),
    ]


async def test_submitting_nothing_says_so_instead_of_erroring(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/web/channel/queue", data={})

    assert response.status_code == 200
    assert "Nothing selected" in response.text
    assert wired_app.state.scheduler.calls == []


async def test_a_forged_selection_value_never_reaches_the_scheduler(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/channel/queue", data={"playlist": [SPRING, "https://evil.com/x"]}
        )

    assert response.status_code == 200
    assert "<strong>422</strong>" in response.text
    assert wired_app.state.scheduler.calls == []


async def test_a_channel_with_no_playlists_says_so(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/web/channel", data={"url": CHANNEL})

    assert response.status_code == 200
    assert "no playlists" in response.text


async def test_a_video_url_is_pointed_back_at_the_main_page(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/channel", data={"url": "https://youtu.be/abcdefghijk"}
        )

    assert response.status_code == 200
    assert "queue it from the main page" in response.text
    assert wired_app.state.channel_runner.calls == []


async def test_an_unrecognisable_url_says_it_is_not_a_channel(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/web/channel", data={"url": "https://evil.com/@x"})

    assert response.status_code == 200
    assert "Not a supported YouTube channel URL." in response.text
    assert wired_app.state.channel_runner.calls == []


async def test_a_failing_lookup_says_to_retry_rather_than_showing_an_empty_channel(
    wired_app,
) -> None:
    wired_app.state.channel_runner.exit_code = 1

    async with await _client(wired_app) as client:
        response = await client.post("/web/channel", data={"url": CHANNEL})

    assert response.status_code == 200
    body = response.text
    assert "Could not read that channel" in body
    assert "no playlists" not in body


async def test_queueing_while_not_ready_flashes_503(wired_app) -> None:
    wired_app.state.telegram = SimpleNamespace(is_connected=False)

    async with await _client(wired_app) as client:
        response = await client.post("/web/channel/queue", data={"playlist": SPRING})

    assert response.status_code == 200
    body = response.text
    assert "<strong>503</strong>" in body
    assert "still connecting" in body
    assert wired_app.state.scheduler.calls == []


async def test_a_channel_url_on_the_main_form_is_sent_to_the_channel_page(
    wired_app,
) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/web/youtube", data={"url": CHANNEL})

    assert response.status_code == 200
    body = response.text
    assert "<strong>422</strong>" in body
    assert "use the channel page" in body
    assert wired_app.state.scheduler.calls == []


async def test_the_json_api_still_refuses_channel_urls(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/jobs/youtube", json={"url": CHANNEL})

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "unsupported_youtube_url"
    assert wired_app.state.scheduler.calls == []
