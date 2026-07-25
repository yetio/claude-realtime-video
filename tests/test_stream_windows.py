"""M2 source-clock and cross-window state regressions."""
import pytest

from claude_real_video.stream_windows import (
    SegmentRunner,
    SourceWindow,
    SourceWatermark,
    TimedEvidence,
    TranscriptReconciler,
    TranscriptSegment,
    WindowDeduplicator,
    WindowEventProducer,
    segment_windows,
)
from claude_real_video.job_events import JOB_STARTED, JobEventBus


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


def test_segment_runner_splits_source_and_emits_monotonic_cross_window_evidence():
    assert segment_windows(5_000, window_ms=2_000) == [
        # Source windows are half-open: [0, 2000), [2000, 4000), [4000, 5000).
        SourceWindow(0, 0, 2_000),
        SourceWindow(1, 2_000, 4_000),
        SourceWindow(2, 4_000, 5_000),
    ]
    runner = SegmentRunner(allowed_lateness_ms=1_000, dedup_ttl_ms=3_000)
    assert runner.frame("scene-a", 1_000, {"artifact": "frames/a.jpg"}) == []
    emitted = runner.transcript(TranscriptSegment(500, 1_200, "opening"))
    emitted += runner.frame("scene-a", 2_000, {"artifact": "frames/a-duplicate.jpg"})
    emitted += runner.frame("scene-b", 3_000, {"artifact": "frames/b.jpg"})
    emitted += runner.finish()
    assert [event.media_time_ms for event in emitted] == sorted(event.media_time_ms for event in emitted)
    assert [event.kind for event in emitted] == [
        "transcript_segment", "frame_kept", "frame_dropped", "frame_kept",
    ]


def test_window_event_producer_uses_the_existing_m1_event_sink_in_source_order():
    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("window-job", JOB_STARTED)
    producer = WindowEventProducer(bus.event_sink("window-job"), allowed_lateness_ms=1_000,
                                   dedup_ttl_ms=3_000)
    producer.frame("scene-a", 1_000, {"artifact": "frames/a.jpg"})
    producer.transcript(TranscriptSegment(500, 1_200, "opening"))
    producer.frame("scene-a", 2_000, {"artifact": "frames/a-repeat.jpg"})
    producer.frame("scene-b", 3_000, {"artifact": "frames/b.jpg"})
    producer.finish()

    events = bus.replay("window-job")
    assert [event.type for event in events] == [
        JOB_STARTED, "transcript_segment", "frame_kept", "frame_dropped", "frame_kept",
    ]
    assert [event.media_time_ms for event in events[1:]] == [500, 1_000, 2_000, 3_000]
    assert events[1].payload == {"text": "opening", "end_time_ms": 1_200}
