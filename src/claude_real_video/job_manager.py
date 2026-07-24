"""Thread-safe ownership of local web jobs, retention and cancellation."""
from __future__ import annotations

from dataclasses import dataclass, field
import os
import shutil
import threading
import time
from typing import Callable
from uuid import uuid4

from .core import ProcessController
from .job_events import (
    JOB_CANCELLED,
    JOB_CLEANUP,
    JOB_DONE,
    JOB_ERROR,
    JOB_STARTED,
    JobEventBus,
)


@dataclass
class ManagedJob:
    job_id: str
    out_dir: str
    bus: JobEventBus
    cancel_event: threading.Event = field(default_factory=threading.Event)
    state: str = "new"
    log: str = ""
    error_code: str | None = None
    created_at: float = field(default_factory=time.monotonic)
    cleaned_at: float | None = None
    clients: int = 0
    done_event: dict | None = None
    controller: ProcessController = field(init=False)

    def __post_init__(self) -> None:
        self.controller = ProcessController(self.cancel_event)


class JobManager:
    """The only owner allowed to transition job state or reclaim job resources."""

    def __init__(self, *, output_root: str | None = None, max_jobs: int = 2,
                 max_clients_per_job: int = 4, retention_seconds: float = 3600,
                 max_job_bytes: int = 512 * 1024 * 1024,
                 max_events_per_job: int = 512) -> None:
        self.output_root = os.path.realpath(output_root or os.path.expanduser("~/crv-web-out"))
        self.max_jobs = max_jobs
        self.max_clients_per_job = max_clients_per_job
        self.retention_seconds = retention_seconds
        self.max_job_bytes = max_job_bytes
        self.max_events_per_job = max_events_per_job
        self._jobs: dict[str, ManagedJob] = {}
        self._lock = threading.RLock()

    def create(self) -> ManagedJob:
        with self._lock:
            self.reap_expired()
            active = sum(job.state in {"new", "running", "cancelling"} for job in self._jobs.values())
            if active >= self.max_jobs:
                raise RuntimeError("job capacity reached")
            job_id = uuid4().hex
            job = ManagedJob(job_id, os.path.join(self.output_root, job_id),
                             JobEventBus(max_events_per_job=self.max_events_per_job))
            self._jobs[job_id] = job
            # Check the active output tree while each child process is running,
            # rather than discovering a quota breach only after it finishes.
            job.controller.quota_check = lambda: self.enforce_quota(job)
            return job

    def get(self, job_id: str | None) -> ManagedJob | None:
        with self._lock:
            self.reap_expired()
            return self._jobs.get(job_id or "")

    def start(self, job: ManagedJob, source_kind: str) -> None:
        with self._lock:
            if job.state != "new":
                raise RuntimeError("job has already started")
            job.state = "running"
            job.bus.emit(job.job_id, JOB_STARTED, {"source_kind": source_kind})

    def request_cancel(self, job: ManagedJob) -> str:
        with self._lock:
            if job.state in {"done", "error", "cancelled", "cleaned"}:
                return job.state
            job.state = "cancelling"
            job.controller.cancel()
            return job.state

    def append_log(self, job: ManagedJob, message: str) -> None:
        with self._lock:
            job.log = (job.log + message + "\n")[-4000:]

    def capture_done(self, job: ManagedJob, payload: dict) -> None:
        with self._lock:
            job.done_event = dict(payload)

    def terminal(self, job: ManagedJob, event_type: str, payload: dict | None = None) -> None:
        with self._lock:
            if job.bus.is_terminal(job.job_id):
                return
            if event_type == JOB_DONE:
                job.state = "done"
            elif event_type == JOB_CANCELLED:
                job.state = "cancelled"
            elif event_type == JOB_ERROR:
                job.state = "error"
                job.error_code = str((payload or {}).get("error_type") or "processing_failed")
            else:
                raise ValueError("terminal event required")
            job.bus.emit(job.job_id, event_type, payload)

    def cleanup(self, job: ManagedJob, reason: str = "worker finished") -> None:
        with self._lock:
            if not job.bus.is_terminal(job.job_id) or job.bus.has_cleanup(job.job_id):
                return
            # Cancellation removes partial artifacts. Successful/error jobs remain
            # replayable until retention expiry, then this manager reclaims them.
            if job.state == "cancelled":
                shutil.rmtree(job.out_dir, ignore_errors=True)
            job.bus.emit(job.job_id, JOB_CLEANUP, {"reason": reason})
            job.cleaned_at = time.monotonic()

    def enforce_quota(self, job: ManagedJob) -> bool:
        with self._lock:
            total = 0
            if os.path.isdir(job.out_dir):
                for root, _dirs, files in os.walk(job.out_dir):
                    for name in files:
                        try:
                            total += os.path.getsize(os.path.join(root, name))
                        except OSError:
                            continue
                        if total > self.max_job_bytes:
                            return False
            return True

    def acquire_client(self, job: ManagedJob) -> bool:
        with self._lock:
            if job.clients >= self.max_clients_per_job:
                return False
            job.clients += 1
            return True

    def release_client(self, job: ManagedJob) -> None:
        with self._lock:
            job.clients = max(0, job.clients - 1)

    def reap_expired(self, now: float | None = None) -> list[str]:
        with self._lock:
            now = time.monotonic() if now is None else now
            expired: list[str] = []
            for job_id, job in list(self._jobs.items()):
                if job.cleaned_at is None or job.clients:
                    continue
                if now - job.cleaned_at < self.retention_seconds:
                    continue
                shutil.rmtree(job.out_dir, ignore_errors=True)
                job.bus.forget(job_id)
                del self._jobs[job_id]
                expired.append(job_id)
            return expired

    def clear_for_tests(self) -> None:
        with self._lock:
            for job in self._jobs.values():
                shutil.rmtree(job.out_dir, ignore_errors=True)
            self._jobs.clear()
