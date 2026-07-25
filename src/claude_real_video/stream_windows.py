"""Shared source-clock, watermark, and cross-window reconciliation state.

M2 producers may receive frames and transcript evidence out of order.  This
module keeps their source-media timeline deterministic before either the local
file segment runner or the M3 RTSP runner emits M1 events.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
import os
from pathlib import Path
import re
import subprocess
from typing import Any, Callable, Hashable, Iterable


@dataclass(frozen=True)
class TimedEvidence:
    """One normalized item on a source-media clock."""

    media_time_ms: int
    kind: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class SourceWindow:
    """One half-open slice of a bounded source timeline."""

    index: int
    start_ms: int
    end_ms: int


@dataclass(frozen=True)
class SegmentFrame:
    """A real frame extracted from one source window on the absolute clock."""

    window: SourceWindow
    media_time_ms: int
    path: str


def segment_windows(duration_ms: int, *, window_ms: int) -> list[SourceWindow]:
    """Split a finite source into contiguous half-open processing windows."""
    _validate_time(duration_ms)
    if window_ms <= 0:
        raise ValueError("window_ms must be > 0")
    return [
        SourceWindow(index, start, min(start + window_ms, duration_ms))
        for index, start in enumerate(range(0, duration_ms, window_ms))
    ]


class SourceWatermark:
    """Release ordered evidence once it is outside the lateness allowance."""

    def __init__(self, *, allowed_lateness_ms: int = 0) -> None:
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be >= 0")
        self.allowed_lateness_ms = allowed_lateness_ms
        self.max_observed_ms: int | None = None
        self.watermark_ms: int | None = None
        self._pending: list[TimedEvidence] = []
        self._last_emitted_ms = -1

    def add(self, evidence: TimedEvidence) -> list[TimedEvidence]:
        """Accept evidence unless it is older than an emitted watermark.

        Returned evidence is sorted by source time.  Late evidence is ignored:
        it must not rewrite an already emitted timeline.
        """
        _validate_time(evidence.media_time_ms)
        if self.watermark_ms is not None and evidence.media_time_ms < self.watermark_ms:
            return []
        self.max_observed_ms = max(self.max_observed_ms or 0, evidence.media_time_ms)
        self.watermark_ms = self.max_observed_ms - self.allowed_lateness_ms
        self._pending.append(evidence)
        return self._drain_through(self.watermark_ms)

    def finish(self) -> list[TimedEvidence]:
        """Flush remaining on-time evidence when a bounded source ends."""
        if self.max_observed_ms is None:
            return []
        return self._drain_through(self.max_observed_ms)

    def _drain_through(self, threshold_ms: int) -> list[TimedEvidence]:
        ready = [item for item in self._pending if item.media_time_ms <= threshold_ms]
        self._pending = [item for item in self._pending if item.media_time_ms > threshold_ms]
        ready.sort(key=lambda item: item.media_time_ms)
        emitted: list[TimedEvidence] = []
        for item in ready:
            if item.media_time_ms < self._last_emitted_ms:
                continue
            emitted.append(item)
            self._last_emitted_ms = item.media_time_ms
        return emitted


class WindowDeduplicator:
    """Suppress repeated frame signatures across windows until their TTL ends."""

    def __init__(self, *, ttl_ms: int) -> None:
        if ttl_ms <= 0:
            raise ValueError("ttl_ms must be > 0")
        self.ttl_ms = ttl_ms
        self._last_kept_ms: dict[Hashable, int] = {}

    def keep(self, signature: Hashable, media_time_ms: int) -> bool:
        _validate_time(media_time_ms)
        previous = self._last_kept_ms.get(signature)
        if previous is not None and media_time_ms < previous + self.ttl_ms:
            return False
        self._last_kept_ms[signature] = media_time_ms
        self._expire_before(media_time_ms - self.ttl_ms)
        return True

    def _expire_before(self, threshold_ms: int) -> None:
        for signature, kept_at in list(self._last_kept_ms.items()):
            if kept_at < threshold_ms:
                del self._last_kept_ms[signature]


@dataclass(frozen=True)
class TranscriptSegment:
    start_ms: int
    end_ms: int
    text: str


class TranscriptReconciler:
    """Merge overlapping segment reads without duplicating cross-window text."""

    def __init__(self) -> None:
        self._seen: set[tuple[int, int, str]] = set()

    def reconcile(self, segments: list[TranscriptSegment]) -> list[TranscriptSegment]:
        unique: list[TranscriptSegment] = []
        for segment in segments:
            _validate_time(segment.start_ms)
            _validate_time(segment.end_ms)
            if segment.end_ms < segment.start_ms:
                raise ValueError("transcript segment end precedes start")
            key = (segment.start_ms, segment.end_ms, segment.text.strip())
            if key in self._seen:
                continue
            self._seen.add(key)
            unique.append(TranscriptSegment(segment.start_ms, segment.end_ms, key[2]))
        return sorted(unique, key=lambda segment: (segment.start_ms, segment.end_ms, segment.text))


class SegmentRunner:
    """Normalize frame/transcript producers onto one watermark-controlled stream.

    The caller can process each ``SourceWindow`` independently, then submit
    evidence here.  Returned events are safe to hand to the M1 event sink in
    order; late evidence is discarded and repeated frame signatures become a
    regular ``frame_dropped`` event until their TTL expires.
    """

    def __init__(self, *, allowed_lateness_ms: int = 0,
                 dedup_ttl_ms: int = 5_000) -> None:
        self.clock = SourceWatermark(allowed_lateness_ms=allowed_lateness_ms)
        self.dedup = WindowDeduplicator(ttl_ms=dedup_ttl_ms)
        self.transcripts = TranscriptReconciler()

    def frame(self, signature: Hashable, media_time_ms: int,
              payload: dict[str, Any] | None = None) -> list[TimedEvidence]:
        event_type = "frame_kept" if self.dedup.keep(signature, media_time_ms) else "frame_dropped"
        return self.clock.add(TimedEvidence(media_time_ms, event_type, dict(payload or {})))

    def transcript(self, segment: TranscriptSegment) -> list[TimedEvidence]:
        reconciled = self.transcripts.reconcile([segment])
        if not reconciled:
            return []
        item = reconciled[0]
        return self.clock.add(TimedEvidence(item.start_ms, "transcript_segment", {
            "end_time_ms": item.end_ms,
            "text": item.text,
        }))

    def finish(self) -> list[TimedEvidence]:
        return self.clock.finish()


class WindowEventProducer:
    """Adapt a M2 ``SegmentRunner`` to the existing M1 ``event_sink`` API.

    Local segment readers and the M3 RTSP sampler can call ``frame`` and
    ``transcript`` as evidence arrives.  The adapter only emits the source-time
    ordered items released by the watermark, so the existing JobEventBus/SSE
    path remains the single public event transport.
    """

    def __init__(self, event_sink: Callable[[str, dict[str, Any]], None],
                 *, allowed_lateness_ms: int = 0,
                 dedup_ttl_ms: int = 5_000) -> None:
        self.event_sink = event_sink
        self.runner = SegmentRunner(
            allowed_lateness_ms=allowed_lateness_ms,
            dedup_ttl_ms=dedup_ttl_ms,
        )

    def frame(self, signature: Hashable, media_time_ms: int,
              payload: dict[str, Any] | None = None) -> None:
        self._emit(self.runner.frame(signature, media_time_ms, payload))

    def transcript(self, segment: TranscriptSegment) -> None:
        self._emit(self.runner.transcript(segment))

    def finish(self) -> None:
        self._emit(self.runner.finish())

    def _emit(self, evidence: list[TimedEvidence]) -> None:
        for item in evidence:
            payload = dict(item.payload)
            payload["timestamp_seconds"] = item.media_time_ms / 1000
            if item.kind == "transcript_segment":
                payload["end_seconds"] = payload.pop("end_time_ms") / 1000
            self.event_sink(item.kind, payload)


def read_local_video_windows(video: str, out_dir: str, *, duration_ms: int,
                             window_ms: int, sample_fps: float = 1.0,
                             command_runner: Callable[[list[str]], subprocess.CompletedProcess[str]] | None = None
                             ) -> Iterable[SegmentFrame]:
    """Extract timestamped frames from a bounded local source one window at a time.

    ``-ss`` makes ffmpeg's filter timestamps relative to the segment, so each
    parsed timestamp is offset by the window start before leaving this function.
    The injectable runner lets callers reuse their cancellation-aware process
    controller instead of starting an unmanaged child process.
    """
    if sample_fps <= 0 or not math.isfinite(sample_fps):
        raise ValueError("sample_fps must be finite and > 0")
    runner = command_runner or _run_command
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    for window in segment_windows(duration_ms, window_ms=window_ms):
        pattern = root / f"window_{window.index:05d}_%05d.jpg"
        command = [
            "ffmpeg", "-y", "-ss", f"{window.start_ms / 1000:.3f}",
            "-t", f"{(window.end_ms - window.start_ms) / 1000:.3f}", "-i", video,
            "-vf", f"fps={sample_fps},showinfo,scale=640:-1", "-vsync", "vfr",
            str(pattern), "-hide_banner", "-loglevel", "info",
        ]
        result = runner(command)
        if result.returncode != 0:
            raise RuntimeError("ffmpeg failed while reading source window")
        paths = sorted(root.glob(f"window_{window.index:05d}_*.jpg"))
        local_times = _showinfo_times(result.stderr)
        if len(local_times) != len(paths):
            local_times = [index / sample_fps for index in range(len(paths))]
        for path, local_seconds in zip(paths, local_times):
            media_time_ms = min(
                window.end_ms - 1,
                window.start_ms + max(0, round(local_seconds * 1000)),
            )
            yield SegmentFrame(window, media_time_ms, str(path))


def emit_local_video_windows(producer: WindowEventProducer, video: str, out_dir: str,
                             *, duration_ms: int, window_ms: int,
                             sample_fps: float = 1.0,
                             command_runner: Callable[[list[str]], subprocess.CompletedProcess[str]] | None = None
                             ) -> None:
    """Read local source windows and publish their deduplicated M1 frame events."""
    root = os.path.realpath(out_dir)
    for frame in read_local_video_windows(
        video, root, duration_ms=duration_ms, window_ms=window_ms,
        sample_fps=sample_fps, command_runner=command_runner,
    ):
        with open(frame.path, "rb") as image:
            signature = hashlib.blake2b(image.read(), digest_size=16).digest()
        producer.frame(signature, frame.media_time_ms, {
            "artifact": os.path.relpath(frame.path, root).replace(os.sep, "/"),
            "selection_reason": "window_sample",
        })
    producer.finish()


def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, errors="replace")


def _showinfo_times(stderr: str) -> list[float]:
    return [max(0.0, float(value)) for value in re.findall(
        r"pts_time:\s*(-?[0-9]+(?:\.[0-9]+)?)", stderr or "",
    )]


def _validate_time(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or not math.isfinite(value):
        raise ValueError("media_time_ms must be a non-negative integer")
