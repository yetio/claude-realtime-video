"""M2 source-clock and cross-window state regressions."""
import json
import subprocess
import pytest

from claude_real_video.stream_windows import (
    ProgressiveWindowCursor,
    SegmentRunner,
    SourceWindow,
    SourceWatermark,
    TimedEvidence,
    TranscriptReconciler,
    TranscriptSegment,
    WindowDeduplicator,
    WindowEventProducer,
    emit_local_media_windows,
    emit_local_video_windows,
    emit_progressive_video_update,
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


def test_progressive_cursor_releases_complete_windows_once_and_final_partial():
    cursor = ProgressiveWindowCursor(window_ms=1_000)
    assert cursor.observe(999) == []
    assert cursor.observe(1_500) == [SourceWindow(0, 0, 1_000)]
    with pytest.raises(ValueError, match="cannot move backwards"):
        cursor.observe(1_499)
    assert cursor.observe(2_100) == [SourceWindow(1, 1_000, 2_000)]
    assert cursor.observe(2_500, final=True) == [SourceWindow(2, 2_000, 2_500)]
    assert cursor.observe(3_000) == []


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


def test_local_segment_reader_emits_real_windowed_frames_to_m1_sink(tmp_path):
    if not _ffmpeg_available():
        pytest.skip("ffmpeg not installed")
    source = tmp_path / "source.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=duration=3:size=160x120:rate=10",
        "-pix_fmt", "yuv420p", str(source),
    ], check=True, capture_output=True)
    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("local-window-job", JOB_STARTED)
    out = tmp_path / "frames"
    producer = WindowEventProducer(bus.event_sink("local-window-job"), dedup_ttl_ms=1)
    emit_local_video_windows(
        producer, str(source), str(out), duration_ms=3_000, window_ms=1_000,
        sample_fps=1.0,
    )
    events = bus.replay("local-window-job")
    frame_events = [event for event in events if event.type == "frame_kept"]
    assert frame_events
    assert [event.media_time_ms for event in frame_events] == sorted(
        event.media_time_ms for event in frame_events)
    assert all(0 <= event.media_time_ms < 3_000 for event in frame_events)
    assert all((out / event.payload["artifact"]).is_file() for event in frame_events)


def test_local_media_windows_reconcile_overlapping_transcript_segments(tmp_path):
    if not _ffmpeg_available():
        pytest.skip("ffmpeg not installed")
    source = tmp_path / "source.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=duration=2:size=160x120:rate=10",
        "-pix_fmt", "yuv420p", str(source),
    ], check=True, capture_output=True)
    transcript = tmp_path / "transcript.json"
    transcript.write_text(json.dumps({"segments": [
        {"start": 0.2, "end": 0.6, "text": "opening"},
        {"start": 0.8, "end": 1.2, "text": "bridge"},
    ]}), encoding="utf-8")
    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("media-window-job", JOB_STARTED)
    producer = WindowEventProducer(bus.event_sink("media-window-job"), dedup_ttl_ms=1)
    emit_local_media_windows(
        producer, str(source), str(tmp_path / "frames"), duration_ms=2_000,
        window_ms=1_000, sample_fps=1.0, transcript_json=str(transcript),
    )
    evidence = [event for event in bus.replay("media-window-job") if event.type != JOB_STARTED]
    assert [event.media_time_ms for event in evidence] == sorted(
        event.media_time_ms for event in evidence)
    transcript_events = [event for event in evidence if event.type == "transcript_segment"]
    assert [event.payload["text"] for event in transcript_events] == ["opening", "bridge"]


def test_progressive_source_emits_each_real_window_once(tmp_path):
    if not _ffmpeg_available():
        pytest.skip("ffmpeg not installed")
    source = tmp_path / "source.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=duration=3:size=160x120:rate=10",
        "-pix_fmt", "yuv420p", str(source),
    ], check=True, capture_output=True)
    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("progressive-job", JOB_STARTED)
    producer = WindowEventProducer(bus.event_sink("progressive-job"), dedup_ttl_ms=1)
    cursor = ProgressiveWindowCursor(window_ms=1_000)
    out = tmp_path / "frames"

    first = emit_progressive_video_update(
        producer, cursor, str(source), str(out), playable_duration_ms=1_000,
    )
    first_count = len(bus.replay("progressive-job"))
    second = emit_progressive_video_update(
        producer, cursor, str(source), str(out), playable_duration_ms=2_000,
    )
    second_count = len(bus.replay("progressive-job"))
    final = emit_progressive_video_update(
        producer, cursor, str(source), str(out), playable_duration_ms=3_000, final=True,
    )
    events = [event for event in bus.replay("progressive-job") if event.type != JOB_STARTED]

    assert first == [SourceWindow(0, 0, 1_000)]
    assert second == [SourceWindow(1, 1_000, 2_000)]
    assert final == [SourceWindow(2, 2_000, 3_000)]
    assert first_count < second_count < len(events) + 1
    assert [event.media_time_ms for event in events] == sorted(
        event.media_time_ms for event in events)


def test_real_multiscene_video_spans_three_source_windows(tmp_path):
    if not _ffmpeg_available():
        pytest.skip("ffmpeg not installed")
    source = tmp_path / "multiscene.mp4"
    subprocess.run([
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", "testsrc=duration=2:size=160x120:rate=10",
        "-f", "lavfi", "-i", "smptebars=duration=2:size=160x120:rate=10",
        "-f", "lavfi", "-i", "rgbtestsrc=duration=2:size=160x120:rate=10",
        "-filter_complex", "[0:v][1:v][2:v]concat=n=3:v=1[v]", "-map", "[v]",
        "-pix_fmt", "yuv420p", str(source),
    ], check=True, capture_output=True)
    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("multiscene-job", JOB_STARTED)
    out = tmp_path / "frames"
    producer = WindowEventProducer(bus.event_sink("multiscene-job"), dedup_ttl_ms=1)
    emit_local_video_windows(
        producer, str(source), str(out), duration_ms=6_000, window_ms=2_000,
        sample_fps=1.0,
    )
    frames = [event for event in bus.replay("multiscene-job") if event.type == "frame_kept"]
    window_ids = {event.payload["artifact"].split("_")[1] for event in frames}
    assert window_ids == {"00000", "00001", "00002"}
    assert [event.media_time_ms for event in frames] == sorted(
        event.media_time_ms for event in frames)


def _ffmpeg_available():
    from shutil import which
    return bool(which("ffmpeg"))
