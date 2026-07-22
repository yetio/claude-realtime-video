"""Regression tests for the realtime job event bus."""

import threading
import time
import unittest

from claude_real_video.job_events import (
    FRAME_KEPT,
    JOB_CANCELLED,
    JOB_CLEANUP,
    JOB_DONE,
    JOB_LOG,
    JOB_STARTED,
    JobEventBus,
)


class JobEventBusTests(unittest.TestCase):
    def test_emit_assigns_monotonic_seq_and_required_fields(self):
        bus = JobEventBus(clock=lambda: 123.0)

        started = bus.emit("job-1", JOB_STARTED, {"source": "file.mp4"})
        frame = bus.emit("job-1", FRAME_KEPT, {"artifact": "frames/raw_00001.jpg"})

        self.assertEqual([e.seq for e in bus.replay("job-1")], [1, 2])
        self.assertEqual(started.to_dict(), {
            "job_id": "job-1",
            "seq": 1,
            "type": JOB_STARTED,
            "ts": 123.0,
            "data": {"source": "file.mp4"},
        })
        self.assertEqual(frame.to_dict()["data"]["artifact"], "frames/raw_00001.jpg")

    def test_replay_since_seq(self):
        bus = JobEventBus(clock=lambda: 1.0)
        bus.emit("job-1", JOB_STARTED)
        bus.emit("job-1", JOB_LOG, {"line": "extracting"})
        bus.emit("job-1", FRAME_KEPT, {"artifact": "frames/raw_00001.jpg"})

        replayed = bus.replay("job-1", since=1)

        self.assertEqual([e.seq for e in replayed], [2, 3])
        self.assertEqual([e.type for e in replayed], [JOB_LOG, FRAME_KEPT])

    def test_bounded_queue_keeps_latest_events(self):
        bus = JobEventBus(max_events_per_job=3, clock=lambda: 1.0)
        for i in range(5):
            bus.emit("job-1", JOB_LOG, {"i": i})

        retained = bus.replay("job-1")

        self.assertEqual([e.seq for e in retained], [3, 4, 5])
        self.assertEqual(bus.first_retained_seq("job-1"), 3)
        self.assertEqual(bus.last_seq("job-1"), 5)

    def test_terminal_blocks_late_non_cleanup_events(self):
        bus = JobEventBus(clock=lambda: 1.0)
        done = bus.emit("job-1", JOB_DONE, {"frames": 2})

        self.assertTrue(done.is_terminal)
        self.assertTrue(bus.is_terminal("job-1"))
        with self.assertRaises(RuntimeError):
            bus.emit("job-1", JOB_LOG, {"line": "too late"})

        cleanup = bus.cleanup("job-1", "removed temp files")
        self.assertEqual(cleanup.type, JOB_CLEANUP)
        self.assertTrue(bus.has_cleanup("job-1"))
        with self.assertRaises(RuntimeError):
            bus.cleanup("job-1", "again")

    def test_cleanup_requires_terminal_event(self):
        bus = JobEventBus(clock=lambda: 1.0)

        with self.assertRaises(RuntimeError):
            bus.cleanup("job-1", "too early")

    def test_cancel_is_terminal_event(self):
        bus = JobEventBus(clock=lambda: 1.0)
        cancelled = bus.cancel("job-1", "user requested")

        self.assertEqual(cancelled.type, JOB_CANCELLED)
        self.assertTrue(cancelled.is_terminal)
        self.assertEqual(cancelled.data, {"reason": "user requested"})

    def test_wait_for_events_unblocks_on_emit(self):
        bus = JobEventBus(clock=lambda: 1.0)
        seen = []

        def waiter():
            seen.extend(bus.wait_for_events("job-1", since=0, timeout=1.0))

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.05)
        bus.emit("job-1", JOB_STARTED)
        thread.join(timeout=1.0)

        self.assertEqual([e.type for e in seen], [JOB_STARTED])

    def test_rejects_invalid_inputs(self):
        bus = JobEventBus(clock=lambda: 1.0)
        with self.assertRaises(ValueError):
            JobEventBus(max_events_per_job=0)
        with self.assertRaises(ValueError):
            bus.emit("", JOB_STARTED)
        with self.assertRaises(ValueError):
            bus.emit("job-1", "unknown")


if __name__ == "__main__":
    unittest.main()
