import os
import tempfile
import threading
import time
import unittest

from task_queue import TaskQueue, UnknownTaskError


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class DeterministicTests(unittest.TestCase):
    """Uses run_once() and a fake clock: no threads, no sleeping."""

    def setUp(self):
        self.clock = FakeClock()
        self.q = TaskQueue(backoff_base=2, clock=self.clock)
        self.addCleanup(self.q.close)

    def test_success_stores_result(self):
        self.q.register("double", lambda p: p * 2)
        tid = self.q.enqueue("double", 21)
        self.assertTrue(self.q.run_once())
        t = self.q.get(tid)
        self.assertEqual((t.status, t.result, t.attempts), ("done", 42, 1))
        self.assertFalse(self.q.run_once())

    def test_fifo_order(self):
        seen = []
        self.q.register("note", seen.append)
        for i in range(5):
            self.q.enqueue("note", i)
        while self.q.run_once():
            pass
        self.assertEqual(seen, [0, 1, 2, 3, 4])

    def test_retries_with_exponential_backoff_then_succeeds(self):
        calls = []

        def flaky(_):
            calls.append(self.clock.now)
            if len(calls) < 3:
                raise ValueError("boom")
            return "finally"

        self.q.register("flaky", flaky)
        tid = self.q.enqueue("flaky", max_retries=3)
        self.assertTrue(self.q.run_once())  # attempt 1 fails
        t = self.q.get(tid)
        self.assertEqual((t.status, t.attempts, t.last_error), ("pending", 1, "ValueError: boom"))
        self.assertAlmostEqual(t.run_at, self.clock.now + 2)  # base * 2**0
        self.assertFalse(self.q.run_once())  # not due yet
        self.clock.now += 2
        self.assertTrue(self.q.run_once())  # attempt 2 fails
        self.assertAlmostEqual(self.q.get(tid).run_at, self.clock.now + 4)  # base * 2**1
        self.clock.now += 3.99
        self.assertFalse(self.q.run_once())
        self.clock.now += 0.01
        self.assertTrue(self.q.run_once())  # attempt 3 succeeds
        t = self.q.get(tid)
        self.assertEqual((t.status, t.result, t.attempts), ("done", "finally", 3))
        self.assertIsNone(t.last_error)

    def test_gives_up_after_max_retries(self):
        self.q.register("bad", lambda _: 1 / 0)
        tid = self.q.enqueue("bad", max_retries=2)
        for _ in range(3):
            self.assertTrue(self.q.run_once())
            self.clock.now += 100
        t = self.q.get(tid)
        self.assertEqual((t.status, t.attempts), ("failed", 3))
        self.assertIn("ZeroDivisionError", t.last_error)
        self.assertFalse(self.q.run_once())

    def test_zero_retries_fails_immediately(self):
        self.q.register("bad", lambda _: 1 / 0)
        tid = self.q.enqueue("bad", max_retries=0)
        self.q.run_once()
        self.assertEqual(self.q.get(tid).status, "failed")

    def test_unknown_handler_is_a_failure_not_a_crash(self):
        tid = self.q.enqueue("nobody", max_retries=0)
        self.q.run_once()
        t = self.q.get(tid)
        self.assertEqual(t.status, "failed")
        self.assertIn("no handler", t.last_error)

    def test_delay(self):
        self.q.register("x", lambda p: p)
        self.q.enqueue("x", 1, delay=30)
        self.assertFalse(self.q.run_once())
        self.clock.now += 30
        self.assertTrue(self.q.run_once())

    def test_non_serialisable_payload_rejected(self):
        with self.assertRaises(TypeError):
            self.q.enqueue("x", object())

    def test_non_serialisable_result_counts_as_failure(self):
        self.q.register("x", lambda _: object())
        tid = self.q.enqueue("x", max_retries=0)
        self.q.run_once()
        self.assertEqual(self.q.get(tid).status, "failed")

    def test_unknown_task(self):
        with self.assertRaises(UnknownTaskError):
            self.q.get(999)

    def test_stats_and_list(self):
        self.q.register("ok", lambda p: p)
        self.q.register("bad", lambda p: 1 / 0)
        self.q.enqueue("ok")
        self.q.enqueue("bad", max_retries=0)
        self.q.enqueue("ok")
        self.q.run_once()
        self.q.run_once()
        self.assertEqual(self.q.stats(), {"pending": 1, "running": 0, "done": 1, "failed": 1})
        self.assertEqual(len(self.q.list("done")), 1)
        self.assertEqual(len(self.q.list()), 3)


class PersistenceTests(unittest.TestCase):
    def test_tasks_survive_restart_and_interrupted_ones_are_requeued(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "q.db")
            q = TaskQueue(path)
            a = q.enqueue("job", "a")
            b = q.enqueue("job", "b")
            claimed = q._claim()  # worker "crashes" while holding task a
            self.assertEqual(claimed.id, a)
            q._db.close()  # no clean shutdown

            q2 = TaskQueue(path)
            self.assertEqual(q2.recovered, 1)
            seen = []
            q2.register("job", seen.append)
            while q2.run_once():
                pass
            self.assertEqual(seen, ["a", "b"])
            self.assertEqual(q2.get(b).status, "done")
            q2.close()


class WorkerPoolTests(unittest.TestCase):
    def test_pool_processes_everything_exactly_once(self):
        q = TaskQueue(poll_interval=0.01)
        self.addCleanup(q.close)
        lock = threading.Lock()
        counts = {}

        def handler(n):
            with lock:
                counts[n] = counts.get(n, 0) + 1
            return n

        q.register("count", handler)
        for i in range(200):
            q.enqueue("count", i)
        q.start(workers=6)
        self.assertTrue(q.wait_idle(timeout=15))
        q.shutdown()
        self.assertEqual(len(counts), 200)
        self.assertTrue(all(v == 1 for v in counts.values()))
        self.assertEqual(q.stats()["done"], 200)

    def test_workers_run_in_parallel(self):
        q = TaskQueue(poll_interval=0.01)
        self.addCleanup(q.close)
        barrier = threading.Barrier(3, timeout=5)
        q.register("meet", lambda _: barrier.wait() is not None)
        for _ in range(3):
            q.enqueue("meet")
        q.start(workers=3)  # only completes if all three run at the same time
        self.assertTrue(q.wait_idle(timeout=10))
        self.assertEqual(q.stats()["done"], 3)

    def test_retry_with_real_threads(self):
        q = TaskQueue(backoff_base=0.02, poll_interval=0.01)
        self.addCleanup(q.close)
        attempts = []

        def flaky(_):
            attempts.append(time.monotonic())
            if len(attempts) < 3:
                raise RuntimeError("not yet")
            return "ok"

        q.register("flaky", flaky)
        tid = q.enqueue("flaky", max_retries=5)
        q.start(workers=2)
        self.assertTrue(q.wait_idle(timeout=10))
        self.assertEqual(q.get(tid).status, "done")
        self.assertEqual(len(attempts), 3)
        self.assertGreaterEqual(attempts[1] - attempts[0], 0.02)
        self.assertGreaterEqual(attempts[2] - attempts[1], 0.04)

    def test_graceful_shutdown_finishes_running_and_keeps_the_rest(self):
        q = TaskQueue(poll_interval=0.01)
        self.addCleanup(q.close)
        started = threading.Event()
        release = threading.Event()
        finished = []

        def slow(n):
            started.set()
            release.wait(5)
            finished.append(n)

        q.register("slow", slow)
        for i in range(5):
            q.enqueue("slow", i)
        q.start(workers=1)
        self.assertTrue(started.wait(5))

        stopper = threading.Thread(target=q.shutdown)
        stopper.start()
        time.sleep(0.1)
        self.assertTrue(stopper.is_alive())  # waiting for the running task
        release.set()
        stopper.join(5)
        self.assertFalse(stopper.is_alive())

        self.assertEqual(finished, [0])  # the in-flight task completed, nothing else started
        stats = q.stats()
        self.assertEqual((stats["done"], stats["pending"], stats["running"]), (1, 4, 0))

    def test_shutdown_timeout_reports_stuck_worker(self):
        q = TaskQueue(poll_interval=0.01)
        self.addCleanup(q.close)
        release = threading.Event()
        started = threading.Event()
        q.register("stuck", lambda _: (started.set(), release.wait(5)))
        q.enqueue("stuck")
        q.start(workers=1)
        started.wait(5)
        self.assertFalse(q.shutdown(timeout=0.1))
        release.set()
        self.assertTrue(q.shutdown(timeout=5))

    def test_cannot_start_twice(self):
        q = TaskQueue()
        self.addCleanup(q.close)
        q.start(1)
        with self.assertRaises(RuntimeError):
            q.start(1)


if __name__ == "__main__":
    unittest.main()
