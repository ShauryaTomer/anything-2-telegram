from collections.abc import Callable
from typing import Protocol


class EventBus(Protocol):
    def on(self, event: str, handler: Callable[..., object]) -> object: ...

    def emit(self, event: str, *args: object) -> bool: ...
