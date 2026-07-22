"""In-process realtime job events for crv-web.

This module is intentionally independent from the video pipeline. M1 wires it
into the web server first, then later commits can make core.process emit events
without changing the public CLI output contract.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from threading import Condition, RLock
import time
from typing import Any, Callable


JOB_STARTED = "job_started"
SOURCE_READY = "source_ready"
FRAME_KEPT = "frame_kept"
FRAME_DROPPED = "frame_dropped"
JOB_LOG = "job_log"
TRANSCRIPT_SEGMENT = "transcript_segment"
JOB_DONE = "job_done"
JOB_ERROR = "job_error"
JOB_CANCELLED = "job_cancelled"
JOB_CLEANUP = "job_cleanup"

EVENT_TYPES = frozenset({
    JOB_STARTED,
    SOURCE_READY,
    FRAME_KEPT,
    FRAME_DROPPED,
    JOB_LOG,
    TRANSCRIPT_SEGMENT,
    JOB_DONE,
    JOB_ERROR,
    JOB_CANCELLED,
    JOB_CLEANUP,
})

TERMINAL_EVENT_TYPES = frozenset({JOB_DONE, JOB_ERROR, JOB_CANCELLED})


@dataclass(frozen=True)
class JobEvent:
    """JSON-serializable event delivered to local realtime viewers."""

    job_id: str
    seq: int
    type: str
    data: dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    @property
    def is_terminal(self) -> bool:
        return self.type in TERMINAL_EVENT_TYPES

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "seq": self.seq,
            "type": self.type,
            "ts": self.ts,
            "data": dict(self.data),
        }


@dataclass
class _JobState:
    events: deque[JobEvent]
    next_seq: int = 1
    terminal_seq: int | None = None
    cleanup_seq: int | None = None


class JobEventBus:
    """Thread-safe bounded event log per job.

    The queue is bounded to prevent a long-running video job from growing memory
    without limit. Consumers can replay events with seq greater than a known
    value; if the requested seq has already fallen out of the bounded queue,
    replay starts at the oldest retained event.
    """

    def __init__(
        self,
        *,
        max_events_per_job: int = 1000,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if max_events_per_job < 1:
            raise ValueError("max_events_per_job must be >= 1")
        self.max_events_per_job = max_events_per_job
        self._clock = clock
        self._lock = RLock()
        self._changed = Condition(self._lock)
        self._jobs: dict[str, _JobState] = {}

    def emit(self, job_id: str, event_type: str, data: dict[str, Any] | None = None) -> JobEvent:
        if not job_id:
            raise ValueError("job_id is required")
        if event_type not in EVENT_TYPES:
            raise ValueError(f"unknown event type: {event_type}")
        payload = dict(data or {})
        with self._changed:
            state = self._state_for(job_id)
            if state.cleanup_seq is not None:
                raise RuntimeError("cannot emit event after cleanup")
            if event_type == JOB_CLEANUP and state.terminal_seq is None:
                raise RuntimeError("cannot cleanup before terminal event")
            if state.terminal_seq is not None and event_type != JOB_CLEANUP:
                raise RuntimeError("cannot emit non-cleanup event after terminal event")
            event = JobEvent(
                job_id=job_id,
                seq=state.next_seq,
                type=event_type,
                data=payload,
                ts=float(self._clock()),
            )
            state.next_seq += 1
            state.events.append(event)
            if event.is_terminal:
                state.terminal_seq = event.seq
            if event.type == JOB_CLEANUP:
                state.cleanup_seq = event.seq
            self._changed.notify_all()
            return event

    def cancel(self, job_id: str, reason: str = "") -> JobEvent:
        data = {"reason": reason} if reason else {}
        return self.emit(job_id, JOB_CANCELLED, data)

    def cleanup(self, job_id: str, reason: str = "") -> JobEvent:
        data = {"reason": reason} if reason else {}
        return self.emit(job_id, JOB_CLEANUP, data)

    def replay(self, job_id: str, *, since: int = 0) -> list[JobEvent]:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None:
                return []
            return [event for event in state.events if event.seq > since]

    def wait_for_events(self, job_id: str, *, since: int = 0, timeout: float | None = None) -> list[JobEvent]:
        with self._changed:
            events = self.replay(job_id, since=since)
            if events:
                return events
            self._changed.wait(timeout=timeout)
            return self.replay(job_id, since=since)

    def last_seq(self, job_id: str) -> int:
        with self._lock:
            state = self._jobs.get(job_id)
            return 0 if state is None else state.next_seq - 1

    def first_retained_seq(self, job_id: str) -> int | None:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None or not state.events:
                return None
            return state.events[0].seq

    def is_terminal(self, job_id: str) -> bool:
        with self._lock:
            state = self._jobs.get(job_id)
            return bool(state and state.terminal_seq is not None)

    def has_cleanup(self, job_id: str) -> bool:
        with self._lock:
            state = self._jobs.get(job_id)
            return bool(state and state.cleanup_seq is not None)

    def forget(self, job_id: str) -> None:
        with self._changed:
            self._jobs.pop(job_id, None)
            self._changed.notify_all()

    def _state_for(self, job_id: str) -> _JobState:
        state = self._jobs.get(job_id)
        if state is None:
            state = _JobState(events=deque(maxlen=self.max_events_per_job))
            self._jobs[job_id] = state
        return state
