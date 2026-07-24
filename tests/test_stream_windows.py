"""M2 source-clock and cross-window state regressions."""
import pytest

from claude_real_video.stream_windows import (
    SourceWatermark,
    TimedEvidence,
    TranscriptReconciler,
    TranscriptSegment,
    WindowDeduplicator,
)


def test_watermark_releases_ordered_evidence_and_rejects_late_items():
    clock = SourceWatermark(allowed_lateness_ms=100)
    assert clock.add(TimedEvidence(1_000, "frame", {"id": "a"})) == []
    assert [item.payload["id"] for item in clock.add(TimedEvidence(1_100, "frame", {"id": "b"}))] == ["a"]
    assert clock.add(TimedEvidence(900, "frame", {"id": "late"})) == []
    assert [item.payload["id"] for item in clock.finish()] == ["b"]


def test_cross_window_dedup_releases_an_unchanged_frame_after_ttl():
    dedup = WindowDeduplicator(ttl_ms=5_000)
    assert dedup.keep("static-camera", 0)
    assert not dedup.keep("static-camera", 4_999)
    assert dedup.keep("static-camera", 5_000)


def test_transcript_reconciliation_removes_window_overlap_and_sorts():
    reconciler = TranscriptReconciler()
    segments = reconciler.reconcile([
        TranscriptSegment(2_000, 3_000, " second "),
        TranscriptSegment(0, 1_000, "first"),
        TranscriptSegment(2_000, 3_000, "second"),
    ])
    assert segments == [
        TranscriptSegment(0, 1_000, "first"),
        TranscriptSegment(2_000, 3_000, "second"),
    ]
    assert reconciler.reconcile([TranscriptSegment(2_000, 3_000, "second")]) == []


def test_window_state_rejects_invalid_source_times():
    with pytest.raises(ValueError):
        SourceWatermark().add(TimedEvidence(-1, "frame", {}))
    with pytest.raises(ValueError):
        WindowDeduplicator(ttl_ms=1).keep("x", True)
