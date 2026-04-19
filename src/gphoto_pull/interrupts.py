"""Cooperative Ctrl-C handling for long-running pull loops.

Description:
    Provides a temporary SIGINT handler that lets pull workers notice
    cancellation and stop browser/download work cleanly.
"""

from __future__ import annotations

import signal
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from types import FrameType


@dataclass(slots=True)
class _InterruptState:
    """Cooperative interrupt state.

    Description:
        Stores process-local Ctrl-C state while the temporary signal handler is
        active.

    Attributes:
        requested: Whether Ctrl-C has been observed in the active handler.
        count: Number of Ctrl-C signals observed in the active handler.
    """

    requested: bool = False
    count: int = 0


_STATE = _InterruptState()
_CALLBACKS: set[Callable[[], None]] = set()


def interrupt_requested() -> bool:
    """Description:
    Report whether SIGINT has been requested inside the cooperative handler.

    Returns:
        `True` after the first Ctrl-C until the handler context exits.
    """

    requested = _STATE.requested
    return requested


def raise_if_interrupt_requested() -> None:
    """Description:
    Convert a pending cooperative interrupt into `KeyboardInterrupt`.

    Side Effects:
        Raises `KeyboardInterrupt` when an interrupt has been requested.
    """

    if _STATE.requested:
        raise KeyboardInterrupt


def _clear_interrupt_request() -> None:
    """Description:
    Reset cooperative interrupt state.

    Side Effects:
        Mutates module-level interrupt state.
    """

    _STATE.requested = False
    _STATE.count = 0


def add_interrupt_callback(callback: Callable[[], None]) -> None:
    """Description:
    Register a callback invoked when Ctrl-C is observed.

    Args:
        callback: No-argument callback to invoke from the signal handler.

    Side Effects:
        Mutates process-local callback state.
    """

    _CALLBACKS.add(callback)


def remove_interrupt_callback(callback: Callable[[], None]) -> None:
    """Description:
    Remove a previously registered Ctrl-C callback.

    Args:
        callback: Callback to remove.

    Side Effects:
        Mutates process-local callback state.
    """

    _CALLBACKS.discard(callback)


@contextmanager
def cooperative_sigint_handling() -> Iterator[None]:
    """Description:
    Install a temporary SIGINT handler for graceful pull cancellation.

    Returns:
        A context manager that sets an interrupt flag on first Ctrl-C and delegates
        to the previous/default handler on repeated Ctrl-C.

    Side Effects:
        Temporarily replaces the process SIGINT handler and resets local interrupt
        state when the context exits.
    """

    previous_handler = signal.getsignal(signal.SIGINT)
    _clear_interrupt_request()

    def handler(signum: int, frame: FrameType | None) -> None:
        """Description:
        Record Ctrl-C once and delegate repeated interrupts to the previous handler.

        Args:
            signum: Signal number.
            frame: Interrupted frame provided by Python's signal module.

        Side Effects:
            Mutates interrupt state and may invoke the previous/default handler.
        """

        _STATE.requested = True
        _STATE.count += 1
        for callback in tuple(_CALLBACKS):
            with suppress(Exception):
                callback()
        if _STATE.count >= 2:
            if callable(previous_handler):
                previous_handler(signum, frame)
                return
            signal.default_int_handler(signum, frame)

    signal.signal(signal.SIGINT, handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous_handler)
        _clear_interrupt_request()
