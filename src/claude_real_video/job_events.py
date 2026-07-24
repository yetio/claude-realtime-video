"""Versioned, bounded realtime job events for the local web viewer."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from threading import Condition, RLock
import os
import time
from typing import Any, Callable


EVENT_SCHEMA_VERSION = 1
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
REPLAY_GAP = "replay_gap"

EVENT_TYPES = frozenset({
    JOB_STARTED, SOURCE_READY, FRAME_KEPT, FRAME_DROPPED, JOB_LOG,
    TRANSCRIPT_SEGMENT, JOB_DONE, JOB_ERROR, JOB_CANCELLED, JOB_CLEANUP,
})
TERMINAL_EVENT_TYPES = frozenset({JOB_DONE, JOB_ERROR, JOB_CANCELLED})


@dataclass(frozen=True)
class JobEvent:
    """Public v1 event. ``payload`` never contains raw media bytes or secrets."""

    job_id: str
    seq: int
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    occurred_at: str = ""
    media_time_ms: int | None = None

    @property
    def is_terminal(self) -> bool:
        return self.type in TERMINAL_EVENT_TYPES

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": EVENT_SCHEMA_VERSION,
            "job_id": self.job_id,
            "seq": self.seq,
            "type": self.type,
            "occurred_at": self.occurred_at,
            "payload": dict(self.payload),
        }
        if self.media_time_ms is not None:
            result["media_time_ms"] = self.media_time_ms
        return result


@dataclass(frozen=True)
class Replay:
    events: list[JobEvent]
    gap: bool = False
    first_retained_seq: int | None = None


@dataclass
class _JobState:
    events: deque[JobEvent]
    next_seq: int = 1
    phase: str = "new"
    terminal_seq: int | None = None
    cleanup_seq: int | None = None


def _relative_artifact(value: Any) -> str | None:
    if not isinstance(value, str) or not value or os.path.isabs(value):
        return None
    normalized = value.replace("\\", "/")
    if normalized.startswith("../") or "/../" in normalized or normalized == "..":
        return None
    return normalized


def _bounded_text(value: Any, *, maximum: int = 2000) -> str:
    return str(value or "")[:maximum]


def _payload_for(event_type: str, data: dict[str, Any]) -> tuple[dict[str, Any], int | None]:
    """Allowlist external payloads; error details and raw media never cross SSE."""
    artifact = _relative_artifact(data.get("artifact"))
    manifest_artifact = _relative_artifact(data.get("manifest_artifact"))
    media_time = data.get("timestamp_seconds")
    media_time_ms = int(float(media_time) * 1000) if isinstance(media_time, (int, float)) else None
    if event_type == JOB_STARTED:
        return {"source_kind": "url" if data.get("source_kind") == "url" else "file"}, None
    if event_type == SOURCE_READY:
        payload = {"duration_seconds": int(data.get("duration_seconds") or 0)}
        if artifact:
            payload["artifact"] = artifact
        return payload, None
    if event_type == FRAME_KEPT:
        payload = {"selection_reason": _bounded_text(data.get("selection_reason"), maximum=80) or "scene"}
        if artifact:
            payload["artifact"] = artifact
        return payload, media_time_ms
    if event_type == FRAME_DROPPED:
        return {
            "reason": _bounded_text(data.get("reason"), maximum=80) or "deduplicated",
            "count": max(1, int(data.get("count") or 1)),
        }, media_time_ms
    if event_type == JOB_LOG:
        return {"message": _bounded_text(data.get("message"))}, None
    if event_type == TRANSCRIPT_SEGMENT:
        return {"text": _bounded_text(data.get("text")), "end_time_ms": int(float(data.get("end_seconds") or 0) * 1000)}, media_time_ms
    if event_type == JOB_DONE:
        return {
            "frame_count": max(0, int(data.get("frame_count") or 0)),
            "extracted_frames": max(0, int(data.get("extracted_frames") or 0)),
            "duration_seconds": max(0, int(data.get("duration_seconds") or 0)),
            **({"manifest_artifact": manifest_artifact} if manifest_artifact else {}),
        }, None
    if event_type == JOB_CANCELLED:
        return {"reason": _bounded_text(data.get("reason"), maximum=160) or "cancelled"}, None
    if event_type == JOB_ERROR:
        # Never disclose source URLs, query credentials, command lines or local paths.
        return {"code": _bounded_text(data.get("error_type"), maximum=80) or "processing_failed"}, None
    if event_type == JOB_CLEANUP:
        return {"reason": _bounded_text(data.get("reason"), maximum=160) or "worker finished"}, None
    raise ValueError(f"unknown event type: {event_type}")


class JobEventBus:
    """Thread-safe per-job replay log with a strict public lifecycle."""

    def __init__(self, *, max_events_per_job: int = 1000,
                 clock: Callable[[], float] = time.time) -> None:
        if max_events_per_job < 2:
            raise ValueError("max_events_per_job must be >= 2")
        self.max_events_per_job = max_events_per_job
        self._clock = clock
        self._lock = RLock()
        self._changed = Condition(self._lock)
        self._jobs: dict[str, _JobState] = {}

    def emit(self, job_id: str, event_type: str,
             data: dict[str, Any] | None = None) -> JobEvent:
        if not job_id:
            raise ValueError("job_id is required")
        if event_type not in EVENT_TYPES:
            raise ValueError(f"unknown event type: {event_type}")
        payload, media_time_ms = _payload_for(event_type, dict(data or {}))
        with self._changed:
            state = self._state_for(job_id)
            self._validate_transition(state, event_type)
            # A run with thousands of rejected frames needs one summary event, not
            # thousands of replay entries. Flush naturally on the next non-drop.
            if event_type == FRAME_DROPPED and state.events and state.events[-1].type == FRAME_DROPPED:
                previous = state.events[-1]
                combined = dict(previous.payload)
                combined["count"] = int(combined["count"]) + int(payload["count"])
                state.events[-1] = replace(previous, payload=combined)
                self._changed.notify_all()
                return state.events[-1]
            occurred_at = datetime.fromtimestamp(
                float(self._clock()), tz=timezone.utc,
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            event = JobEvent(job_id, state.next_seq, event_type, payload, occurred_at, media_time_ms)
            state.next_seq += 1
            state.events.append(event)
            if event_type == JOB_STARTED:
                state.phase = "started"
            elif event.is_terminal:
                state.phase, state.terminal_seq = "terminal", event.seq
            elif event_type == JOB_CLEANUP:
                state.phase, state.cleanup_seq = "cleaned", event.seq
            self._changed.notify_all()
            return event

    def _validate_transition(self, state: _JobState, event_type: str) -> None:
        if state.phase == "new" and event_type != JOB_STARTED:
            raise RuntimeError("first event must be job_started")
        if state.phase == "started" and event_type == JOB_STARTED:
            raise RuntimeError("job_started may only be emitted once")
        if state.phase == "terminal" and event_type != JOB_CLEANUP:
            raise RuntimeError("cannot emit after terminal event")
        if state.phase == "cleaned":
            raise RuntimeError("cannot emit after cleanup")
        if event_type == JOB_CLEANUP and state.phase != "terminal":
            raise RuntimeError("cleanup requires exactly one terminal event")

    def event_sink(self, job_id: str) -> Callable[[str, dict[str, Any]], None]:
        if not job_id:
            raise ValueError("job_id is required")
        return lambda event_type, data: self.emit(job_id, event_type, data)

    def cancel(self, job_id: str, reason: str = "") -> JobEvent:
        return self.emit(job_id, JOB_CANCELLED, {"reason": reason})

    def cleanup(self, job_id: str, reason: str = "") -> JobEvent:
        return self.emit(job_id, JOB_CLEANUP, {"reason": reason})

    def read_replay(self, job_id: str, *, since: int = 0) -> Replay:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None:
                return Replay([])
            first = state.events[0].seq if state.events else None
            gap = bool(first is not None and since < first - 1)
            return Replay([event for event in state.events if event.seq > since], gap, first)

    def replay(self, job_id: str, *, since: int = 0) -> list[JobEvent]:
        return self.read_replay(job_id, since=since).events

    def wait_for_events(self, job_id: str, *, since: int = 0,
                        timeout: float | None = None) -> Replay:
        with self._changed:
            replay = self.read_replay(job_id, since=since)
            if replay.events or replay.gap:
                return replay
            self._changed.wait(timeout=timeout)
            return self.read_replay(job_id, since=since)

    def last_seq(self, job_id: str) -> int:
        with self._lock:
            state = self._jobs.get(job_id)
            return 0 if state is None else state.next_seq - 1

    def first_retained_seq(self, job_id: str) -> int | None:
        with self._lock:
            state = self._jobs.get(job_id)
            return state.events[0].seq if state and state.events else None

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
        return self._jobs.setdefault(job_id, _JobState(deque(maxlen=self.max_events_per_job)))
