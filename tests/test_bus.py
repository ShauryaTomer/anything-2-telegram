from anything2telegram.bus import EventBus


def test_event_bus_protocol_has_minimal_on_surface() -> None:
    assert callable(EventBus.on)
    assert callable(EventBus.emit)
