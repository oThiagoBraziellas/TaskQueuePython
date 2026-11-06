"""Command line interface: enqueue, work, status, list."""

import argparse
import json
import signal
import sys
import threading
from typing import List, Optional

from .handlers import DEMO
from .queue import TaskQueue


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="task_queue", description="SQLite-backed task queue")
    p.add_argument("--db", default="tasks.db", help="SQLite file (default: tasks.db)")
    sub = p.add_subparsers(dest="command", required=True)

    e = sub.add_parser("enqueue", help="add a task (demo handlers: echo, sleep, flaky, fail)")
    e.add_argument("name")
    e.add_argument("payload", nargs="?", default="null", help="JSON payload")
    e.add_argument("--retries", type=int, default=3)
    e.add_argument("--delay", type=float, default=0.0)

    w = sub.add_parser("work", help="run workers until interrupted (or until the queue is drained)")
    w.add_argument("--workers", type=int, default=4)
    w.add_argument("--backoff-base", type=float, default=1.0)
    w.add_argument("--drain", action="store_true", help="exit once nothing is pending or running")

    sub.add_parser("status", help="count tasks per status")
    lst = sub.add_parser("list", help="list tasks")
    lst.add_argument("--status", choices=["pending", "running", "done", "failed"])
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    queue = TaskQueue(args.db, backoff_base=getattr(args, "backoff_base", 1.0))
    try:
        if args.command == "enqueue":
            try:
                payload = json.loads(args.payload)
            except json.JSONDecodeError as exc:
                print(f"error: payload is not valid JSON: {exc}", file=sys.stderr)
                return 2
            print(queue.enqueue(args.name, payload, max_retries=args.retries, delay=args.delay))
        elif args.command == "status":
            for status, n in queue.stats().items():
                print(f"{status:<8} {n}")
        elif args.command == "list":
            for t in queue.list(args.status):
                err = f"  error={t.last_error}" if t.last_error else ""
                print(f"#{t.id:<4} {t.status:<8} {t.name:<8} attempts={t.attempts}/{t.max_retries + 1}{err}")
        else:
            for name, fn in DEMO.items():
                queue.register(name, fn)
            if queue.recovered:
                print(f"requeued {queue.recovered} interrupted task(s)")
            stop = threading.Event()
            previous = {
                sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGINT, signal.SIGTERM)
            }
            queue.start(args.workers)
            print(f"{args.workers} workers running; Ctrl+C for graceful shutdown")
            while not stop.is_set():
                if args.drain and queue.wait_idle(timeout=0.2):
                    break
                stop.wait(0.2)
            print("shutting down, waiting for running tasks...")
            queue.shutdown()
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            for status, n in queue.stats().items():
                print(f"{status:<8} {n}")
    finally:
        queue.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
