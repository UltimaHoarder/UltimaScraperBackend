"""Secret-free, atomic readiness projection for the Docker worker probe."""

import json
import os
import time
from pathlib import Path
from uuid import uuid4

import psutil


class WorkerReadiness:
    def __init__(self, path):
        self.path = Path(path) if path else None
        self.generation = uuid4().hex
        self.last_write = float("-inf")
        self.process_started = psutil.Process().create_time()

    def publish(self, backend_id, expected, worker_ids, *, force=False):
        if self.path is None or (not force and time.monotonic() - self.last_write < 10):
            return
        payload = {
            "version": 1,
            "generation": self.generation,
            "pid": os.getpid(),
            "process_started": self.process_started,
            "backend_id": backend_id,
            "expected_workers": expected,
            "worker_ids": sorted(set(worker_ids)),
            "updated_monotonic": time.monotonic(),
        }
        temporary = self.path.with_name(self.path.name + "." + self.generation)
        try:
            descriptor = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600
            )
            with os.fdopen(descriptor, "w") as stream:
                json.dump(payload, stream, sort_keys=True)
            os.replace(temporary, self.path)
            self.last_write = time.monotonic()
        finally:
            temporary.unlink(missing_ok=True)

    def close(self):
        if self.path is None:
            return
        try:
            if json.loads(self.path.read_text()).get("generation") == self.generation:
                self.path.unlink(missing_ok=True)
        except OSError, ValueError:
            # Do not remove a marker whose generation cannot be established,
            # and never let optional readiness cleanup interrupt shutdown.
            pass
