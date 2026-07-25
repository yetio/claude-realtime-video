"""Lifecycle, schema and replay boundary regressions for realtime events."""
import threading
import time
import unittest

from claude_real_video.job_events import (
    EVENT_SCHEMA_VERSION, FRAME_DROPPED, FRAME_KEPT, JOB_CANCELLED,
    JOB_CLEANUP, JOB_DONE, JOB_LOG, JOB_STARTED, JobEventBus,
)


class JobEventBusTests(unittest.TestCase):
    def test_v1_schema_and_lifecycle_are_explicit(self):
        bus = JobEventBus(clock=lambda: 123.0)
        with self.assertRaisesRegex(RuntimeError, "first event"):
            bus.emit("job-1", JOB_LOG, {"message": "too early"})
        started = bus.emit("job-1", JOB_STARTED, {"source_kind": "file"})
        frame = bus.emit("job-1", FRAME_KEPT, {"artifact": "frames/raw.jpg", "timestamp_seconds": 1.25})
        self.assertEqual(started.to_dict(), {
            "schema_version": EVENT_SCHEMA_VERSION, "job_id": "job-1", "seq": 1,
            "type": JOB_STARTED, "occurred_at": "1970-01-01T00:02:03.000Z",
            "payload": {"source_kind": "file"},
        })
        self.assertEqual(frame.to_dict()["payload"]["artifact"], "frames/raw.jpg")
        self.assertEqual(frame.to_dict()["media_time_ms"], 1250)
        with self.assertRaisesRegex(RuntimeError, "only be emitted once"):
            bus.emit("job-1", JOB_STARTED)

    def test_terminal_and_cleanup_are_exactly_once(self):
        bus = JobEventBus(clock=lambda: 1.0)
        bus.emit("job-1", JOB_STARTED)
        done = bus.emit("job-1", JOB_DONE, {"frame_count": 2})
        self.assertTrue(done.is_terminal)
        with self.assertRaisesRegex(RuntimeError, "after terminal"):
            bus.emit("job-1", JOB_LOG, {"message": "late"})
        bus.cleanup("job-1", "retained")
        with self.assertRaisesRegex(RuntimeError, "after cleanup"):
            bus.cleanup("job-1", "twice")
        self.assertEqual([event.type for event in bus.replay("job-1")],
                         [JOB_STARTED, JOB_DONE, JOB_CLEANUP])

    def test_every_terminal_type_obeys_the_same_lifecycle(self):
        for event_type in (JOB_DONE, "job_error", JOB_CANCELLED):
            with self.subTest(event_type=event_type):
                bus = JobEventBus(clock=lambda: 1.0)
                bus.emit(event_type, JOB_STARTED)
                bus.emit(event_type, event_type, {"error_type": "failure"})
                bus.cleanup(event_type)
                self.assertEqual(
                    [event.type for event in bus.replay(event_type)],
                    [JOB_STARTED, event_type, JOB_CLEANUP],
                )

    def test_start_before_cancel_and_error_payload_redaction(self):
        bus = JobEventBus(clock=lambda: 1.0)
        bus.emit("job-1", JOB_STARTED)
        cancelled = bus.cancel("job-1", "user requested")
        self.assertEqual(cancelled.payload, {"reason": "user requested"})
        bus = JobEventBus(clock=lambda: 1.0)
        bus.emit("job-2", JOB_STARTED)
        error = bus.emit("job-2", "job_error", {
            "error_type": "RuntimeError", "message": "https://x/?token=secret /private/a",
        })
        self.assertEqual(error.payload, {"code": "RuntimeError"})

    def test_rtsp_stream_events_are_allowlisted_without_source_details(self):
        bus = JobEventBus(clock=lambda: 1.0)
        bus.emit("rtsp", JOB_STARTED, {"source_kind": "rtsp"})
        started = bus.emit("rtsp", "stream_started", {
            "transport": "tcp", "attempt": 2,
            "source": "rtsp://fixture:fixture-pass@host/live",
        })
        failed = bus.emit("rtsp", "stream_error", {
            "code": "rtsp_auth_failed",
            "stderr": "rtsp://fixture:fixture-pass@host/live",
        })

        self.assertEqual(started.payload, {"transport": "tcp", "attempt": 2})
        self.assertEqual(failed.payload, {"code": "rtsp_auth_failed"})
        self.assertNotIn("fixture-pass", str(started.to_dict()))
        self.assertNotIn("fixture-pass", str(failed.to_dict()))

    def test_bounded_log_reports_replay_gap(self):
        bus = JobEventBus(max_events_per_job=3, clock=lambda: 1.0)
        bus.emit("job-1", JOB_STARTED)
        for i in range(4):
            bus.emit("job-1", JOB_LOG, {"message": str(i)})
        replay = bus.read_replay("job-1", since=0)
        self.assertTrue(replay.gap)
        self.assertEqual(replay.first_retained_seq, 3)
        self.assertEqual([event.seq for event in replay.events], [3, 4, 5])

    def test_frame_drops_are_aggregated(self):
        bus = JobEventBus(clock=lambda: 1.0)
        bus.emit("job-1", JOB_STARTED)
        bus.emit("job-1", FRAME_DROPPED, {"reason": "deduplicated"})
        bus.emit("job-1", FRAME_DROPPED, {"reason": "deduplicated"})
        events = bus.replay("job-1")
        self.assertEqual([event.type for event in events], [JOB_STARTED, FRAME_DROPPED])
        self.assertEqual(events[-1].payload["count"], 2)

    def test_wait_unblocks_with_replay(self):
        bus = JobEventBus(clock=lambda: 1.0)
        seen = []
        thread = threading.Thread(target=lambda: seen.extend(
            bus.wait_for_events("job-1", since=0, timeout=1.0).events))
        thread.start()
        time.sleep(0.05)
        bus.emit("job-1", JOB_STARTED)
        thread.join(timeout=1.0)
        self.assertEqual([event.type for event in seen], [JOB_STARTED])
