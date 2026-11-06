import random
import unittest

from task_queue import backoff_delay


class BackoffTests(unittest.TestCase):
    def test_exponential_growth(self):
        self.assertEqual([backoff_delay(n, base=1) for n in range(1, 6)], [1, 2, 4, 8, 16])

    def test_cap(self):
        self.assertEqual(backoff_delay(20, base=1, cap=30), 30)

    def test_custom_factor(self):
        self.assertEqual(backoff_delay(3, base=2, factor=3), 18)

    def test_jitter_stays_in_range_and_varies(self):
        rng = random.Random(1)
        values = [backoff_delay(3, base=1, jitter=0.25, rng=rng) for _ in range(200)]
        self.assertTrue(all(3 <= v <= 5 for v in values))
        self.assertGreater(len(set(values)), 50)

    def test_invalid(self):
        for kwargs in ({"attempt": 0}, {"attempt": 1, "factor": 0.5}, {"attempt": 1, "jitter": 1}):
            with self.assertRaises(ValueError):
                backoff_delay(**kwargs)


if __name__ == "__main__":
    unittest.main()
