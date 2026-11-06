import contextlib
import io
import os
import tempfile
import unittest

from task_queue.cli import main


def run(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = main(list(argv))
    return rc, out.getvalue(), err.getvalue()


class CliTests(unittest.TestCase):
    def test_enqueue_work_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "cli.db")
            self.assertEqual(run("--db", db, "enqueue", "echo", '{"a": 1}')[0], 0)
            self.assertEqual(run("--db", db, "enqueue", "fail", "null", "--retries", "0")[0], 0)
            rc, out, _ = run("--db", db, "work", "--workers", "2", "--drain", "--backoff-base", "0.01")
            self.assertEqual(rc, 0)
            self.assertIn("done     1", out)
            self.assertIn("failed   1", out)
            rc, out, _ = run("--db", db, "list", "--status", "failed")
            self.assertIn("always fails", out)

    def test_bad_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(run("--db", os.path.join(tmp, "x.db"), "enqueue", "echo", "{oops")[0], 2)


if __name__ == "__main__":
    unittest.main()
