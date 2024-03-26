import time
from unittest import TestCase

from sdc11073.intervaltimer import IntervalTimer


class TestIntervalTimer(TestCase):
    """Tests for IntervalTimer."""

    def test_precision(self):
        """Verify that period is handled correctly."""
        period = 0.1
        loops = 100
        it = IntervalTimer(period_in_seconds=period)
        start = time.perf_counter()
        for _ in range(loops):
            it.wait_next_interval_begin()
        elapsed = time.perf_counter() - start
        self.assertAlmostEqual(elapsed, period * loops, delta=0.05)

    def test_delay(self):
        """Verify that delay calculation is correct."""
        period = 0.1
        loops = 50
        too_long = 0.1
        it = IntervalTimer(period_in_seconds=period)
        behind = it.wait_next_interval_begin()
        self.assertEqual(behind, 0)
        for _ in range(loops):
            time.sleep(period + too_long)  # sleep a little longer than period, timer cannot catch up.
            behind = it.wait_next_interval_begin()
            print(behind)
        self.assertGreater(behind, loops * too_long)
        self.assertLess(behind, loops * too_long + 0.4)
