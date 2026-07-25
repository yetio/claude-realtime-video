"""Shared source-clock, watermark, and cross-window reconciliation state.

M2 producers may receive frames and transcript evidence out of order.  This
module keeps their source-media timeline deterministic before either the local
file segment runner or the M3 RTSP runner emits M1 events.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
from typing import Any, Callable, Hashable, Iterable


class WindowStateOverflow(RuntimeError):
    """Raised when bounded rolling-window state reaches its hard limit."""


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


class ProgressiveWindowCursor:
    """Track which complete source windows are newly playable as a file grows."""

    def __init__(self, *, window_ms: int) -> None:
        if window_ms <= 0:
            raise ValueError("window_ms must be > 0")
        self.window_ms = window_ms
        self.next_start_ms = 0
        self.next_index = 0
        self.max_observed_ms = 0
        self.finished = False

    def observe(self, playable_duration_ms: int, *, final: bool = False) -> list[SourceWindow]:
        """Return only windows not returned by an earlier observation.

        Non-final observations release complete windows.  ``final=True`` also
        releases the last partial window and seals the cursor.
        """
        _validate_time(playable_duration_ms)
        if self.finished:
            return []
        if playable_duration_ms < self.max_observed_ms:
            raise ValueError("playable duration cannot move backwards")
        self.max_observed_ms = playable_duration_ms
        limit = (playable_duration_ms if final else
                 playable_duration_ms - playable_duration_ms % self.window_ms)
        windows: list[SourceWindow] = []
        while self.next_start_ms + self.window_ms <= limit:
            end = self.next_start_ms + self.window_ms
            windows.append(SourceWindow(self.next_index, self.next_start_ms, end))
            self.next_index += 1
            self.next_start_ms = end
        if final and self.next_start_ms < playable_duration_ms:
            windows.append(SourceWindow(
                self.next_index, self.next_start_ms, playable_duration_ms,
            ))
            self.next_index += 1
            self.next_start_ms = playable_duration_ms
        if final:
            self.finished = True
        return windows


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
    """Release ordered evidence once it is outside the lateness allowance.

    The watermark is the earliest still-open source timestamp, so evidence at
    exactly the watermark remains admissible. Equal timestamps retain arrival
    order. Pending state is bounded; overflow fails the job instead of silently
    dropping or reordering evidence.
    """

    def __init__(self, *, allowed_lateness_ms: int = 0,
                 max_pending: int = 1_024,
                 max_pending_bytes: int = 1024 * 1024) -> None:
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be >= 0")
        if max_pending <= 0:
            raise ValueError("max_pending must be > 0")
        if max_pending_bytes <= 0:
            raise ValueError("max_pending_bytes must be > 0")
        self.allowed_lateness_ms = allowed_lateness_ms
        self.max_pending = max_pending
        self.max_pending_bytes = max_pending_bytes
        self.max_observed_ms: int | None = None
        self.watermark_ms: int | None = None
        self._pending: list[tuple[TimedEvidence, int]] = []
        self._pending_bytes = 0
        self._last_emitted_ms = -1
        self._epoch_floor_ms = 0

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def pending_bytes(self) -> int:
        return self._pending_bytes

    def add(self, evidence: TimedEvidence) -> list[TimedEvidence]:
        """Accept evidence unless it is older than an emitted watermark.

        Returned evidence is sorted by source time.  Late evidence is ignored:
        it must not rewrite an already emitted timeline.
        """
        _validate_time(evidence.media_time_ms)
        if self.watermark_ms is not None and evidence.media_time_ms < self.watermark_ms:
            return []
        size = _evidence_size(evidence)
        if size > self.max_pending_bytes:
            raise WindowStateOverflow("one pending evidence item exceeds the byte limit")
        next_max = max(self.max_observed_ms or 0, evidence.media_time_ms)
        next_watermark = max(
            self._epoch_floor_ms, next_max - self.allowed_lateness_ms,
        )
        remaining = [(item, item_size) for item, item_size in self._pending
                     if item.media_time_ms > next_watermark]
        if evidence.media_time_ms > next_watermark:
            remaining.append((evidence, size))
        remaining_bytes = sum(item_size for _item, item_size in remaining)
        if len(remaining) > self.max_pending or remaining_bytes > self.max_pending_bytes:
            raise WindowStateOverflow("pending evidence capacity exceeded")
        self.max_observed_ms = next_max
        self.watermark_ms = next_watermark
        self._pending.append((evidence, size))
        self._pending_bytes += size
        return self._drain_through(self.watermark_ms)

    def finish(self) -> list[TimedEvidence]:
        """Flush remaining on-time evidence when a bounded source ends."""
        if self.max_observed_ms is None:
            return []
        return self._drain_through(self.max_observed_ms)

    def reset_epoch(self, epoch_start_ms: int) -> None:
        """Discard pending evidence and open a new monotonic source epoch.

        Callers must normalize a reconnected source onto the existing absolute
        media clock. Clock rewind below the last emitted timestamp is rejected.
        """
        _validate_time(epoch_start_ms)
        if epoch_start_ms < self._last_emitted_ms:
            raise ValueError("source epoch cannot move behind emitted evidence")
        self._pending.clear()
        self._pending_bytes = 0
        self.max_observed_ms = None
        self._epoch_floor_ms = epoch_start_ms
        self.watermark_ms = self._epoch_floor_ms

    def _drain_through(self, threshold_ms: int) -> list[TimedEvidence]:
        ready = [(item, size) for item, size in self._pending
                 if item.media_time_ms <= threshold_ms]
        self._pending = [(item, size) for item, size in self._pending
                         if item.media_time_ms > threshold_ms]
        self._pending_bytes -= sum(size for _item, size in ready)
        ready.sort(key=lambda row: row[0].media_time_ms)
        emitted: list[TimedEvidence] = []
        for item, _size in ready:
            if item.media_time_ms < self._last_emitted_ms:
                continue
            emitted.append(item)
            self._last_emitted_ms = item.media_time_ms
        return emitted


class WindowDeduplicator:
    """Suppress repeated frame signatures across windows until their TTL ends."""

    def __init__(self, *, ttl_ms: int, max_signatures: int = 4_096) -> None:
        if ttl_ms <= 0:
            raise ValueError("ttl_ms must be > 0")
        if max_signatures <= 0:
            raise ValueError("max_signatures must be > 0")
        self.ttl_ms = ttl_ms
        self.max_signatures = max_signatures
        self._last_kept_ms: dict[Hashable, int] = {}
        self._max_seen_ms = -1

    @property
    def signature_count(self) -> int:
        return len(self._last_kept_ms)

    def keep(self, signature: Hashable, media_time_ms: int) -> bool:
        _validate_time(media_time_ms)
        if media_time_ms < self._max_seen_ms:
            raise ValueError("dedup source time cannot move backwards")
        self._max_seen_ms = media_time_ms
        self._expire_at_or_before(media_time_ms - self.ttl_ms)
        previous = self._last_kept_ms.get(signature)
        if previous is not None and media_time_ms < previous + self.ttl_ms:
            return False
        if previous is None and len(self._last_kept_ms) >= self.max_signatures:
            raise WindowStateOverflow("frame signature capacity exceeded")
        self._last_kept_ms[signature] = media_time_ms
        return True

    def reset_epoch(self) -> None:
        self._last_kept_ms.clear()
        self._max_seen_ms = -1

    def _expire_at_or_before(self, threshold_ms: int) -> None:
        for signature, kept_at in list(self._last_kept_ms.items()):
            if kept_at <= threshold_ms:
                del self._last_kept_ms[signature]


@dataclass(frozen=True)
class TranscriptSegment:
    start_ms: int
    end_ms: int
    text: str
    source_id: str | None = None


class TranscriptReconciler:
    """Merge repeated segment reads using a stable source id when available."""

    def __init__(self, *, max_keys: int = 4_096) -> None:
        if max_keys <= 0:
            raise ValueError("max_keys must be > 0")
        self.max_keys = max_keys
        self._seen: set[Hashable] = set()

    @property
    def key_count(self) -> int:
        return len(self._seen)

    def reconcile(self, segments: list[TranscriptSegment]) -> list[TranscriptSegment]:
        unique: list[TranscriptSegment] = []
        for segment in segments:
            _validate_time(segment.start_ms)
            _validate_time(segment.end_ms)
            if segment.end_ms < segment.start_ms:
                raise ValueError("transcript segment end precedes start")
            text = segment.text.strip()
            source_id = (segment.source_id or "").strip() or None
            key: Hashable = (("source_id", source_id) if source_id is not None else
                             ("exact", segment.start_ms, segment.end_ms, text))
            if key in self._seen:
                continue
            if len(self._seen) >= self.max_keys:
                raise WindowStateOverflow("transcript idempotency capacity exceeded")
            self._seen.add(key)
            unique.append(TranscriptSegment(
                segment.start_ms, segment.end_ms, text, source_id,
            ))
        return sorted(unique, key=lambda segment: (segment.start_ms, segment.end_ms, segment.text))

    def reset_epoch(self) -> None:
        self._seen.clear()


class SegmentRunner:
    """Normalize frame/transcript producers onto one watermark-controlled stream.

    The caller can process each ``SourceWindow`` independently, then submit
    evidence here.  Returned events are safe to hand to the M1 event sink in
    order; late evidence is discarded and repeated frame signatures become a
    regular ``frame_dropped`` event until their TTL expires.
    """

    _FRAME_CANDIDATE = "_frame_candidate"
    _TRANSCRIPT_CANDIDATE = "_transcript_candidate"

    def __init__(self, *, allowed_lateness_ms: int = 0,
                 dedup_ttl_ms: int = 5_000,
                 max_pending: int = 1_024,
                 max_pending_bytes: int = 1024 * 1024,
                 max_signatures: int = 4_096,
                 max_transcript_keys: int = 4_096) -> None:
        self.clock = SourceWatermark(
            allowed_lateness_ms=allowed_lateness_ms,
            max_pending=max_pending,
            max_pending_bytes=max_pending_bytes,
        )
        self.dedup = WindowDeduplicator(
            ttl_ms=dedup_ttl_ms, max_signatures=max_signatures,
        )
        self.transcripts = TranscriptReconciler(max_keys=max_transcript_keys)

    def frame(self, signature: Hashable, media_time_ms: int,
              payload: dict[str, Any] | None = None) -> list[TimedEvidence]:
        released = self.clock.add(TimedEvidence(media_time_ms, self._FRAME_CANDIDATE, {
            "signature": signature,
            "payload": dict(payload or {}),
        }))
        return self._release(released)

    def transcript(self, segment: TranscriptSegment) -> list[TimedEvidence]:
        _validate_time(segment.start_ms)
        _validate_time(segment.end_ms)
        if segment.end_ms < segment.start_ms:
            raise ValueError("transcript segment end precedes start")
        released = self.clock.add(TimedEvidence(segment.start_ms, self._TRANSCRIPT_CANDIDATE, {
            "end_time_ms": segment.end_ms,
            "text": segment.text,
            "source_id": segment.source_id,
        }))
        return self._release(released)

    def finish(self) -> list[TimedEvidence]:
        return self._release(self.clock.finish())

    def reset_epoch(self, epoch_start_ms: int) -> None:
        """Reset retry/reconnect state without allowing the public clock to rewind."""
        self.clock.reset_epoch(epoch_start_ms)
        self.dedup.reset_epoch()
        self.transcripts.reset_epoch()

    def _release(self, evidence: list[TimedEvidence]) -> list[TimedEvidence]:
        released: list[TimedEvidence] = []
        for item in evidence:
            if item.kind == self._FRAME_CANDIDATE:
                event_type = ("frame_kept" if self.dedup.keep(
                    item.payload["signature"], item.media_time_ms,
                ) else "frame_dropped")
                released.append(TimedEvidence(
                    item.media_time_ms, event_type, dict(item.payload["payload"]),
                ))
                continue
            if item.kind == self._TRANSCRIPT_CANDIDATE:
                segments = self.transcripts.reconcile([TranscriptSegment(
                    item.media_time_ms,
                    item.payload["end_time_ms"],
                    item.payload["text"],
                    item.payload.get("source_id"),
                )])
                for segment in segments:
                    released.append(TimedEvidence(
                        segment.start_ms, "transcript_segment", {
                            "end_time_ms": segment.end_ms,
                            "text": segment.text,
                        },
                    ))
                continue
            released.append(item)
        return released


class WindowEventProducer:
    """Adapt a M2 ``SegmentRunner`` to the existing M1 ``event_sink`` API.

    Local segment readers and the M3 RTSP sampler can call ``frame`` and
    ``transcript`` as evidence arrives.  The adapter only emits the source-time
    ordered items released by the watermark, so the existing JobEventBus/SSE
    path remains the single public event transport.
    """

    def __init__(self, event_sink: Callable[[str, dict[str, Any]], None],
                 *, allowed_lateness_ms: int = 0,
                 dedup_ttl_ms: int = 5_000,
                 max_pending: int = 1_024,
                 max_pending_bytes: int = 1024 * 1024,
                 max_signatures: int = 4_096,
                 max_transcript_keys: int = 4_096) -> None:
        self.event_sink = event_sink
        self.runner = SegmentRunner(
            allowed_lateness_ms=allowed_lateness_ms,
            dedup_ttl_ms=dedup_ttl_ms,
            max_pending=max_pending,
            max_pending_bytes=max_pending_bytes,
            max_signatures=max_signatures,
            max_transcript_keys=max_transcript_keys,
        )

    def frame(self, signature: Hashable, media_time_ms: int,
              payload: dict[str, Any] | None = None) -> None:
        self._emit(self.runner.frame(signature, media_time_ms, payload))

    def transcript(self, segment: TranscriptSegment) -> None:
        self._emit(self.runner.transcript(segment))

    def finish(self) -> None:
        self._emit(self.runner.finish())

    def reset_epoch(self, epoch_start_ms: int) -> None:
        self.runner.reset_epoch(epoch_start_ms)

    def _emit(self, evidence: list[TimedEvidence]) -> None:
        for item in evidence:
            payload = dict(item.payload)
            payload["timestamp_seconds"] = item.media_time_ms / 1000
            if item.kind == "transcript_segment":
                payload["end_seconds"] = payload.pop("end_time_ms") / 1000
            self.event_sink(item.kind, payload)


def read_local_video_windows(video: str, out_dir: str, *, duration_ms: int,
                             window_ms: int, sample_fps: float = 1.0,
                             command_runner: Callable[[list[str]], subprocess.CompletedProcess[str]]
                             ) -> Iterable[SegmentFrame]:
    """Extract timestamped frames from a bounded local source one window at a time.

    ``-ss`` makes ffmpeg's filter timestamps relative to the segment, so each
    parsed timestamp is offset by the window start before leaving this function.
    The required runner must be owned by the caller's cancellation-aware
    process controller; this module never starts an unmanaged child process.
    """
    if sample_fps <= 0 or not math.isfinite(sample_fps):
        raise ValueError("sample_fps must be finite and > 0")
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    for window in segment_windows(duration_ms, window_ms=window_ms):
        yield from _read_local_video_window(video, root, window, sample_fps, command_runner)


def emit_local_video_windows(producer: WindowEventProducer, video: str, out_dir: str,
                             *, duration_ms: int, window_ms: int,
                             sample_fps: float = 1.0,
                             command_runner: Callable[[list[str]], subprocess.CompletedProcess[str]]
                             ) -> None:
    """Read local source windows and publish their deduplicated M1 frame events."""
    emit_local_media_windows(
        producer, video, out_dir, duration_ms=duration_ms, window_ms=window_ms,
        sample_fps=sample_fps, command_runner=command_runner,
    )


def read_transcript_json(path: str) -> list[TranscriptSegment]:
    """Load the project's timestamped transcript format on the source clock."""
    try:
        with open(path, encoding="utf-8") as transcript_file:
            rows = json.load(transcript_file).get("segments") or []
    except (OSError, ValueError, TypeError, AttributeError):
        return []
    segments: list[TranscriptSegment] = []
    for row in rows:
        try:
            segment = TranscriptSegment(
                round(float(row["start"]) * 1000),
                round(float(row["end"]) * 1000),
                str(row.get("text") or "").strip(),
                str(row.get("id") or row.get("source_id") or "").strip() or None,
            )
            _validate_time(segment.start_ms)
            _validate_time(segment.end_ms)
            if segment.end_ms >= segment.start_ms and segment.text:
                segments.append(segment)
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    return sorted(segments, key=lambda item: (item.start_ms, item.end_ms, item.text))


def emit_local_media_windows(producer: WindowEventProducer, video: str, out_dir: str,
                             *, duration_ms: int, window_ms: int,
                             sample_fps: float = 1.0,
                             transcript_json: str | None = None,
                             command_runner: Callable[[list[str]], subprocess.CompletedProcess[str]]
                             ) -> None:
    """Progressively merge real frame and transcript evidence window by window."""
    root = os.path.realpath(out_dir)
    Path(root).mkdir(parents=True, exist_ok=True)
    transcripts = read_transcript_json(transcript_json) if transcript_json else []
    for window in segment_windows(duration_ms, window_ms=window_ms):
        frames = list(_read_local_video_window(
            video, Path(root), window, sample_fps, command_runner,
        ))
        window_transcripts = [segment for segment in transcripts
                              if segment.start_ms < window.end_ms and segment.end_ms > window.start_ms]
        _publish_window(producer, root, frames, window_transcripts)
    producer.finish()


def emit_progressive_video_update(producer: WindowEventProducer, cursor: ProgressiveWindowCursor,
                                  video: str, out_dir: str, *, playable_duration_ms: int,
                                  sample_fps: float = 1.0,
                                  transcript_json: str | None = None,
                                  final: bool = False,
                                  command_runner: Callable[[list[str]], subprocess.CompletedProcess[str]]
                                  ) -> list[SourceWindow]:
    """Process only newly playable windows from a progressively growing source."""
    root = os.path.realpath(out_dir)
    Path(root).mkdir(parents=True, exist_ok=True)
    transcripts = read_transcript_json(transcript_json) if transcript_json else []
    windows = cursor.observe(playable_duration_ms, final=final)
    for window in windows:
        frames = list(_read_local_video_window(
            video, Path(root), window, sample_fps, command_runner,
        ))
        window_transcripts = [segment for segment in transcripts
                              if segment.start_ms < window.end_ms and segment.end_ms > window.start_ms]
        _publish_window(producer, root, frames, window_transcripts)
    if final:
        producer.finish()
    return windows


def _publish_window(producer: WindowEventProducer, root: str,
                    frames: list[SegmentFrame],
                    transcripts: list[TranscriptSegment]) -> None:
    evidence = [(frame.media_time_ms, 0, "frame", frame) for frame in frames]
    evidence.extend((segment.start_ms, 1, "transcript", segment)
                    for segment in transcripts)
    for _time, _order, kind, item in sorted(evidence, key=lambda row: (row[0], row[1])):
        if kind == "transcript":
            producer.transcript(item)
            continue
        with open(item.path, "rb") as image:
            signature = hashlib.blake2b(image.read(), digest_size=16).digest()
        producer.frame(signature, item.media_time_ms, {
            "artifact": os.path.relpath(item.path, root).replace(os.sep, "/"),
            "selection_reason": "window_sample",
        })


def _read_local_video_window(video: str, root: Path, window: SourceWindow,
                             sample_fps: float,
                             runner: Callable[[list[str]], subprocess.CompletedProcess[str]]
                             ) -> Iterable[SegmentFrame]:
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


def _showinfo_times(stderr: str) -> list[float]:
    return [max(0.0, float(value)) for value in re.findall(
        r"pts_time:\s*(-?[0-9]+(?:\.[0-9]+)?)", stderr or "",
    )]


def _validate_time(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or not math.isfinite(value):
        raise ValueError("media_time_ms must be a non-negative integer")


def _evidence_size(evidence: TimedEvidence) -> int:
    return (32 + len(evidence.kind.encode("utf-8")) +
            len(repr(evidence.payload).encode("utf-8", errors="replace")))
