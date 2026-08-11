from httpx import ASGITransport, AsyncClient

from anything2telegram.api.jobs import create_jobs_app


async def _client() -> AsyncClient:
    app = create_jobs_app()
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://service")


async def test_page_renders_headers_empty_state_and_flash_placeholder() -> None:
    async with await _client() as client:
        response = await client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    body = response.text
    assert '<div id="flash"></div>' in body
    assert '<div id="queue">' in body
    assert "<th>Source</th>" in body
    assert "<th>Status</th>" in body
    assert "<th>Progress</th>" in body
    assert "<th>Link</th>" in body
    assert "Nothing queued. Paste a YouTube URL or pick a file above." in body
    assert '<meta name="color-scheme" content="light dark">' in body


async def test_page_has_two_forms_with_expected_fields() -> None:
    async with await _client() as client:
        response = await client.get("/")

    body = response.text
    assert body.count("<form") == 2
    assert 'name="url"' in body
    assert 'name="offset"' in body
    assert 'name="file"' in body


async def test_vendored_htmx_is_served_locally() -> None:
    async with await _client() as client:
        response = await client.get("/web/static/vendor/htmx-2.0.9.min.js")

    assert response.status_code == 200
    assert "javascript" in response.headers["content-type"]


async def test_vendored_pico_is_served_locally() -> None:
    async with await _client() as client:
        response = await client.get("/web/static/vendor/pico-2.1.1.classless.min.css")

    assert response.status_code == 200
    assert "css" in response.headers["content-type"]


async def test_existing_json_routes_still_resolve() -> None:
    async with await _client() as client:
        health = await client.get("/health")
        openapi = await client.get("/openapi.json")
        docs = await client.get("/docs")
        missing_job = await client.get("/jobs/not-a-uuid")

    assert health.status_code == 503
    assert health.json() == {"ready": False, "telegram_connected": False}
    assert openapi.status_code == 200
    assert docs.status_code == 200
    assert missing_job.status_code == 404
    assert missing_job.json()["detail"]["code"] == "job_not_found"
