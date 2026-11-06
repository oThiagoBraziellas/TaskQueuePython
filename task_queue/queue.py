"""SQLite-backed task queue and worker pool."""

import json
import sqlite3
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .backoff import backoff_delay

PENDING, RUNNING, DONE, FAILED = "pending", "running", "done", "failed"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    payload     TEXT    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL,
    run_at      REAL    NOT NULL,
    result      TEXT,
    last_error  TEXT,
    created_at  REAL    NOT NULL,
    updated_at  REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(status, run_at, id);
"""


class UnknownTaskError(KeyError):
    """No task with the requested id."""


@dataclass(frozen=True)
class Task:
    id: int
    name: str
    payload: Any
    status: str
    attempts: int
    max_retries: int
    run_at: float
    result: Any
    last_error: Optional[str]


def _row_to_task(row) -> Task:
    return Task(
        id=row[0],
        name=row[1],
        payload=json.loads(row[2]),
        status=row[3],
        attempts=row[4],
        max_retries=row[5],
        run_at=row[6],
        result=json.loads(row[7]) if row[7] is not None else None,
        last_error=row[8],
    )


_COLUMNS = "id, name, payload, status, attempts, max_retries, run_at, result, last_error"


class TaskQueue:
    """Durable queue: tasks live in SQLite, workers are threads.

    * ``enqueue`` stores a task; it survives restarts.
    * ``start(workers)`` launches the pool; each worker atomically claims the
      oldest due task, runs the registered handler and records the outcome.
    * A handler that raises is retried up to ``max_retries`` times with
      exponential backoff; afterwards the task is marked ``failed`` (a dead
      letter that stays in the table for inspection).
    * ``shutdown`` stops gracefully: running tasks finish, nothing new is
      claimed. Tasks left ``running`` by a crash are returned to ``pending``
      the next time a queue is opened (at-least-once delivery, so handlers
      should be idempotent).
    """

    def __init__(
        self,
        path: str = ":memory:",
        backoff_base: float = 1.0,
        backoff_factor: float = 2.0,
        backoff_cap: float = 300.0,
        backoff_jitter: float = 0.0,
        poll_interval: float = 0.05,
        clock: Callable[[], float] = time.time,
    ):
        self._clock = clock
        self._backoff = (backoff_base, backoff_factor, backoff_cap, backoff_jitter)
        self._poll = poll_interval
        self._db_lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.executescript(_SCHEMA)
        self._handlers: Dict[str, Callable[[Any], Any]] = {}
        self._threads: List[threading.Thread] = []
        self._stop = threading.Event()
        self._wake = threading.Condition()
        self._active = 0
        self.recovered = self._requeue_interrupted()

    # -- registration and submission ---------------------------------------

    def register(self, name: str, handler: Optional[Callable[[Any], Any]] = None):
        """Register ``handler`` for ``name``; usable as a decorator."""
        if handler is None:
            def decorator(fn):
                self._handlers[name] = fn
                return fn

            return decorator
        self._handlers[name] = handler
        return handler

    def enqueue(self, name: str, payload: Any = None, max_retries: int = 3, delay: float = 0.0) -> int:
        if max_retries < 0 or delay < 0:
            raise ValueError("max_retries and delay must be non-negative")
        now = self._clock()
        data = json.dumps(payload)  # fail early on non-serialisable payloads
        with self._db_lock:
            cur = self._db.execute(
                "INSERT INTO tasks(name, payload, max_retries, run_at, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (name, data, max_retries, now + delay, now, now),
            )
        with self._wake:
            self._wake.notify()
        return cur.lastrowid

    def get(self, task_id: int) -> Task:
        with self._db_lock:
            row = self._db.execute(f"SELECT {_COLUMNS} FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise UnknownTaskError(task_id)
        return _row_to_task(row)

    def list(self, status: Optional[str] = None) -> List[Task]:
        query = f"SELECT {_COLUMNS} FROM tasks"
        args: tuple = ()
        if status:
            query += " WHERE status = ?"
            args = (status,)
        with self._db_lock:
            rows = self._db.execute(query + " ORDER BY id", args).fetchall()
        return [_row_to_task(r) for r in rows]

    def stats(self) -> Dict[str, int]:
        with self._db_lock:
            rows = self._db.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status").fetchall()
        counts = {PENDING: 0, RUNNING: 0, DONE: 0, FAILED: 0}
        counts.update(dict(rows))
        return counts

    # -- claiming and executing ----------------------------------------------

    def _requeue_interrupted(self) -> int:
        with self._db_lock:
            cur = self._db.execute(
                "UPDATE tasks SET status = 'pending', updated_at = ? WHERE status = 'running'",
                (self._clock(),),
            )
            return cur.rowcount

    def _claim(self) -> Optional[Task]:
        now = self._clock()
        with self._db_lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute(
                    f"SELECT {_COLUMNS} FROM tasks WHERE status = 'pending' AND run_at <= ?"
                    " ORDER BY run_at, id LIMIT 1",
                    (now,),
                ).fetchone()
                if row is None:
                    self._db.execute("COMMIT")
                    return None
                self._db.execute(
                    "UPDATE tasks SET status = 'running', updated_at = ? WHERE id = ?", (now, row[0])
                )
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return _row_to_task(row)

    def _finish(self, task: Task, error: Optional[str], result: Any = None) -> None:
        now = self._clock()
        attempts = task.attempts + 1
        with self._db_lock:
            if error is None:
                self._db.execute(
                    "UPDATE tasks SET status='done', attempts=?, result=?, last_error=NULL, updated_at=?"
                    " WHERE id=?",
                    (attempts, json.dumps(result), now, task.id),
                )
            elif attempts > task.max_retries:
                self._db.execute(
                    "UPDATE tasks SET status='failed', attempts=?, last_error=?, updated_at=? WHERE id=?",
                    (attempts, error, now, task.id),
                )
            else:
                delay = backoff_delay(attempts, *self._backoff)
                self._db.execute(
                    "UPDATE tasks SET status='pending', attempts=?, last_error=?, run_at=?, updated_at=?"
                    " WHERE id=?",
                    (attempts, error, now + delay, now, task.id),
                )

    def _execute(self, task: Task) -> None:
        handler = self._handlers.get(task.name)
        if handler is None:
            self._finish(task, f"no handler registered for {task.name!r}")
            return
        try:
            result = handler(task.payload)
            json.dumps(result)  # results must be storable
        except Exception as exc:
            last_line = traceback.format_exception_only(type(exc), exc)[-1].strip()
            self._finish(task, last_line)
        else:
            self._finish(task, None, result)

    def run_once(self) -> bool:
        """Claim and run one due task in the calling thread; False if none is due.

        Useful for tests and for cron-style draining without a pool.
        """
        task = self._claim()
        if task is None:
            return False
        self._execute(task)
        return True

    # -- worker pool -----------------------------------------------------------

    def start(self, workers: int = 4) -> None:
        if workers <= 0:
            raise ValueError("workers must be positive")
        if self._threads:
            raise RuntimeError("workers already started")
        self._stop.clear()
        for i in range(workers):
            t = threading.Thread(target=self._worker, name=f"task-worker-{i}", daemon=True)
            t.start()
            self._threads.append(t)

    def _worker(self) -> None:
        while not self._stop.is_set():
            task = self._claim()
            if task is None:
                with self._wake:
                    if not self._stop.is_set():
                        self._wake.wait(self._poll)
                continue
            with self._wake:
                self._active += 1
            try:
                self._execute(task)
            finally:
                with self._wake:
                    self._active -= 1
                    self._wake.notify_all()

    def wait_idle(self, timeout: Optional[float] = None) -> bool:
        """Block until no task is pending, retrying or running (failed ones do not count)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            counts = self.stats()
            if counts[PENDING] == 0 and counts[RUNNING] == 0:
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            with self._wake:
                self._wake.wait(self._poll)

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None) -> bool:
        """Stop the pool. Running tasks complete; pending ones stay in the database.

        Returns True when every worker has exited. With ``timeout`` it can
        return False if a handler is still running.
        """
        self._stop.set()
        with self._wake:
            self._wake.notify_all()
        if not wait:
            return not any(t.is_alive() for t in self._threads)
        deadline = None if timeout is None else time.monotonic() + timeout
        for t in self._threads:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            t.join(remaining)
        alive = any(t.is_alive() for t in self._threads)
        if not alive:
            self._threads = []
        return not alive

    def close(self) -> None:
        self.shutdown()
        with self._db_lock:
            self._db.close()

    def __enter__(self) -> "TaskQueue":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
