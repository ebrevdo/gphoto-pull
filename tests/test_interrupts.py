# pyright: reportPrivateUsage=false

import signal
import unittest

from gphoto_pull.interrupts import (
    cooperative_sigint_handling,
    interrupt_requested,
    raise_if_interrupt_requested,
)


class InterruptHandlingTests(unittest.TestCase):
    def test_first_sigint_sets_interrupt_requested(self) -> None:
        with cooperative_sigint_handling():
            handler = signal.getsignal(signal.SIGINT)
            self.assertTrue(callable(handler))
            assert callable(handler)

            handler(signal.SIGINT, None)

            self.assertTrue(interrupt_requested())
            with self.assertRaises(KeyboardInterrupt):
                raise_if_interrupt_requested()

    def test_second_sigint_raises_keyboard_interrupt(self) -> None:
        with cooperative_sigint_handling():
            handler = signal.getsignal(signal.SIGINT)
            self.assertTrue(callable(handler))
            assert callable(handler)

            handler(signal.SIGINT, None)
            with self.assertRaises(KeyboardInterrupt):
                handler(signal.SIGINT, None)


if __name__ == "__main__":
    unittest.main()
