"""Shared source-clock, watermark, and cross-window reconciliation state.

M2 producers may receive frames and transcript evidence out of order.  This
module keeps their source-media timeline deterministic before either the local
file segment runner or the M3 RTSP runner emits M1 events.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Hashable


@dataclass(frozen=True)
class TimedEvidence:
    """One normalized item on a source-media clock."""

    media_time_ms: int
    kind: str
    payload: dict[str, Any]


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


def _validate_time(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or not math.isfinite(value):
        raise ValueError("media_time_ms must be a non-negative integer")
