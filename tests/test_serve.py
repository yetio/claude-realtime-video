"""HTTP integration tests for the local realtime viewer and job ownership."""
from __future__ import annotations

import http.client
import json
import os
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from claude_real_video import serve
from claude_real_video.core import ProcessController, ProcessingCancelled
from claude_real_video.job_events import JOB_CANCELLED, JOB_CLEANUP, JOB_DONE, JOB_LOG, JOB_STARTED, SOURCE_READY
from claude_real_video.job_manager import JobManager
from claude_real_video.rtsp import RtspCaptureError


@pytest.fixture
def manager(tmp_path, monkeypatch):
    instance = JobManager(output_root=str(tmp_path / "out"), retention_seconds=0.01, max_events_per_job=32)
    monkeypatch.setattr(serve, "MANAGER", instance)
    return instance


def _server():
    server = serve.http.server.ThreadingHTTPServer(("127.0.0.1", 0), serve._Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _get(server, path, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=8)
    conn.request("GET", path, headers=headers or {})
    response = conn.getresponse()
    body = response.read()
    headers = dict(response.getheaders())
    conn.close()
    return response.status, headers, body


def _post(server, path, payload=None):
    body = json.dumps(payload).encode() if payload is not None else None
    conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=8)
    conn.request("POST", path, body=body, headers={"Content-Type": "application/json"} if body else {})
    response = conn.getresponse()
    data = response.read()
    status = response.status
    conn.close()
    return status, json.loads(data)


def _started_job(manager, kind="file"):
    job = manager.create()
    manager.start(job, kind)
    return job


def test_sse_replay_and_gap_signal(manager):
    job = _started_job(manager)
    job.bus.emit(job.job_id, SOURCE_READY, {"artifact": "source.mp4"})
    manager.terminal(job, JOB_DONE, {"frame_count": 1})
    server = _server()
    try:
        status, headers, body = _get(server, f"/events?id={job.job_id}&since=1")
        text = body.decode()
        assert status == 200 and headers["Content-Type"].startswith("text/event-stream")
        assert "event: source_ready" in text and "event: job_done" in text
        status, _headers, body = _get(server, f"/events?id={job.job_id}", {"Last-Event-ID": "2"})
        assert status == 200 and "event: job_done" in body.decode()
    finally:
        server.shutdown(); server.server_close()


def test_sse_replays_terminal_appended_after_first_snapshot(manager, monkeypatch):
    job = _started_job(manager)
    original_wait = job.bus.wait_for_events
    first_snapshot = True

    def wait_for_events(job_id, *, since=0, timeout=None):
        nonlocal first_snapshot
        replay = original_wait(job_id, since=since, timeout=timeout)
        if first_snapshot:
            first_snapshot = False
            manager.terminal(job, JOB_DONE, {"frame_count": 1})
        return replay

    monkeypatch.setattr(job.bus, "wait_for_events", wait_for_events)
    server = _server()
    try:
        status, _headers, body = _get(server, f"/events?id={job.job_id}")
        text = body.decode()
        assert status == 200
        assert text.index("event: job_started") < text.index("event: job_done")
        assert text.count("event: job_done") == 1
    finally:
        server.shutdown(); server.server_close()


def test_sse_last_event_id_can_replay_cleanup_only(manager):
    manager.retention_seconds = 60
    job = _started_job(manager)
    manager.terminal(job, JOB_DONE, {"frame_count": 1})
    manager.cleanup(job)
    server = _server()
    try:
        status, _headers, body = _get(
            server,
            f"/events?id={job.job_id}",
            {"Last-Event-ID": "2"},
        )
        text = body.decode()
        assert status == 200
        assert "event: job_cleanup" in text
        assert "event: job_done" not in text
        assert "event: job_started" not in text
    finally:
        server.shutdown(); server.server_close()


def test_sse_replay_gap_is_explicit(manager):
    job = _started_job(manager)
    for index in range(40):
        job.bus.emit(job.job_id, JOB_LOG, {"message": str(index)})
    server = _server()
    try:
        status, _headers, body = _get(server, f"/events?id={job.job_id}&since=0")
        assert status == 200 and "event: replay_gap" in body.decode()
    finally:
        server.shutdown(); server.server_close()


def test_artifacts_reject_escape_variants_and_serve_image(manager, tmp_path):
    job = _started_job(manager)
    os.makedirs(job.out_dir, exist_ok=True)
    frames = os.path.join(job.out_dir, "frames")
    os.mkdir(frames)
    image = os.path.join(frames, "frame.jpg")
    open(image, "wb").write(b"jpeg")
    html = os.path.join(job.out_dir, "untrusted.html")
    open(html, "w").write("<script>alert(1)</script>")
    outside = tmp_path / "out-old" / "secret.txt"
    outside.parent.mkdir(); outside.write_text("secret")
    os.symlink(outside, os.path.join(job.out_dir, "escape"))
    server = _server()
    try:
        status, headers, _body = _get(server, f"/artifacts?id={job.job_id}&path=frames/frame.jpg")
        assert status == 200
        assert headers["Content-Type"] == "image/jpeg"
        assert headers["Content-Disposition"].startswith("inline;")
        for artifact in ("../out-old/secret.txt", str(outside), "escape", "missing.jpg"):
            assert _get(server, f"/artifacts?id={job.job_id}&path={artifact}")[0] == 404
        assert _get(server, f"/artifacts?id={job.job_id}&path=untrusted.html")[0] == 404
    finally:
        server.shutdown(); server.server_close()


def test_worker_owns_terminal_during_cancel_done_race(manager, monkeypatch):
    job = _started_job(manager)

    def fake_process(_src, out_dir, *, event_sink, **_kwargs):
        event_sink(JOB_STARTED, {"source_kind": "file"})
        event_sink(JOB_LOG, {"message": "extracting"})
        serve.MANAGER.request_cancel(job)
        event_sink(JOB_DONE, {"frame_count": 1})
        return SimpleNamespace(frames_dir=out_dir, video=out_dir + "/source.mp4", frame_count=1)

    monkeypatch.setattr(serve, "process", fake_process)
    monkeypatch.setattr(serve, "write_viewer", lambda *_args: "viewer.html")
    serve._run_job(job.job_id, "input.mp4", {"grid": False, "transcribe": False})
    events = job.bus.replay(job.job_id)
    assert job.state == "cancelled"
    assert [event.type for event in events] == [JOB_STARTED, JOB_LOG, JOB_CANCELLED, JOB_CLEANUP]


def test_rtsp_source_kind_and_worker_error_code_are_stable(manager, monkeypatch):
    assert serve._source_kind("rtsp://fixture:fixture-pass@camera/live") == "rtsp"
    job = _started_job(manager, "rtsp")

    def fail_rtsp(*_args, **_kwargs):
        raise RtspCaptureError("stream_timeout")

    monkeypatch.setattr(serve, "process", fail_rtsp)
    serve._run_job(job.job_id, "rtsp://fixture:fixture-pass@camera/live", {})
    events = job.bus.replay(job.job_id)
    assert job.error_code == "stream_timeout"
    assert [event.type for event in events] == [JOB_STARTED, "job_error", JOB_CLEANUP]
    assert "fixture-pass" not in json.dumps([event.to_dict() for event in events])
    assert "document.getElementById('src').value=''" in serve.PAGE


def test_cancellation_wins_after_worker_preterminal_check(manager):
    """Lock the exact window: cancel lands after worker's last check, before done."""
    job = _started_job(manager)
    worker_checked = threading.Event()
    allow_done = threading.Event()

    def worker():
        assert not job.cancel_event.is_set()
        worker_checked.set()
        assert allow_done.wait(timeout=1)
        manager.terminal(job, JOB_DONE, {"frame_count": 1})

    thread = threading.Thread(target=worker)
    thread.start()
    assert worker_checked.wait(timeout=1)
    assert manager.request_cancel(job) == "cancelling"
    allow_done.set()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert job.state == "cancelled"
    assert [event.type for event in job.bus.replay(job.job_id)] == [
        JOB_STARTED, JOB_CANCELLED,
    ]


def test_cancel_endpoint_only_requests_intent(manager):
    job = _started_job(manager)
    server = _server()
    try:
        status, payload = _post(server, f"/cancel?id={job.job_id}")
        assert status == 200 and payload == {"state": "cancelling"}
        assert job.cancel_event.is_set()
        assert [event.type for event in job.bus.replay(job.job_id)] == [JOB_STARTED]
    finally:
        server.shutdown(); server.server_close()


def test_cancellable_process_group_stops_child_work():
    controller = ProcessController()
    outcome = []

    def run():
        try:
            controller.run([sys.executable, "-c", "import time; time.sleep(30)"])
        except ProcessingCancelled:
            outcome.append("cancelled")

    thread = threading.Thread(target=run)
    thread.start(); time.sleep(0.1); controller.cancel(); thread.join(timeout=3)
    assert outcome == ["cancelled"] and not thread.is_alive()


def test_cancellable_process_group_leaves_no_child(tmp_path):
    """Cancellation targets the child process group, not only its parent."""
    pid_file = tmp_path / "child.pid"
    script = (
        "import subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
        f"open({str(pid_file)!r}, 'w').write(str(child.pid))\n"
        "time.sleep(30)\n"
    )
    controller = ProcessController()
    outcome = []

    def run():
        try:
            controller.run([sys.executable, "-c", script])
        except ProcessingCancelled:
            outcome.append("cancelled")

    thread = threading.Thread(target=run)
    thread.start()
    for _ in range(30):
        if pid_file.exists():
            break
        time.sleep(0.05)
    assert pid_file.exists(), "parent did not start its child"
    child_pid = int(pid_file.read_text())
    controller.cancel(); thread.join(timeout=3)
    assert outcome == ["cancelled"] and not thread.is_alive()
    for _ in range(30):
        if not _process_exists(child_pid):
            break
        time.sleep(0.05)
    else:
        pytest.fail(f"orphan child process remains: {child_pid}")


def test_process_controller_interrupt_leaves_no_child(tmp_path):
    pid_file = tmp_path / "interrupt-child.pid"
    script = (
        f"open({str(pid_file)!r}, 'w').write(str(__import__('os').getpid()))\n"
        "import time; time.sleep(30)\n"
    )

    class InterruptEvent:
        @staticmethod
        def is_set():
            return False

        @staticmethod
        def wait(_timeout):
            time.sleep(0.1)
            raise KeyboardInterrupt

    controller = ProcessController()
    controller.cancel_event = InterruptEvent()
    with pytest.raises(KeyboardInterrupt):
        controller.run([sys.executable, "-c", script])
    assert pid_file.exists()
    child_pid = int(pid_file.read_text())
    assert not _process_exists(child_pid)
    assert controller._active is None


def test_manager_bounds_ids_clients_retention_and_quota(tmp_path):
    instance = JobManager(output_root=str(tmp_path / "out"), max_jobs=32,
                          max_clients_per_job=1, retention_seconds=0.01,
                          max_job_bytes=2)
    jobs = []
    threads = [threading.Thread(target=lambda: jobs.append(instance.create())) for _ in range(32)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert len({job.job_id for job in jobs}) == 32

    job = jobs[0]
    os.makedirs(job.out_dir)
    open(os.path.join(job.out_dir, "too-large.bin"), "wb").write(b"123")
    assert not instance.enforce_quota(job)
    instance.start(job, "file")
    instance.terminal(job, JOB_DONE, {})
    instance.cleanup(job)
    assert instance.acquire_client(job)
    assert not instance.acquire_client(job)
    assert instance.reap_expired(now=job.cleaned_at + 1) == []
    instance.release_client(job)
    assert instance.reap_expired(now=job.cleaned_at + 1) == [job.job_id]
    assert job.bus.replay(job.job_id) == []


def test_tiny_video_http_sse_e2e(manager, tmp_path):
    manager.retention_seconds = 60
    if not shutil_which("ffmpeg"):
        pytest.skip("ffmpeg not installed")
    source = tmp_path / "tiny.mp4"
    subprocess.run(["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=duration=1:size=160x120:rate=5",
                    "-pix_fmt", "yuv420p", str(source)], check=True, capture_output=True)
    server = _server()
    try:
        status, created = _post(server, "/run", {"src": str(source), "opts": {"grid": False, "transcribe": False}})
        assert status == 200
        status, _headers, body = _get(server, f"/events?id={created['id']}")
        text = body.decode()
        assert status == 200
        assert "event: job_started" in text and "event: frame_kept" in text and "event: job_done" in text
        payloads = [json.loads(line[6:]) for line in text.splitlines() if line.startswith("data: ")]
        frame = next(payload for payload in payloads if payload["type"] == "frame_kept")
        assert frame["schema_version"] == 1
        assert frame["payload"]["artifact"].startswith("frames/")
        assert "media_time_ms" in frame
        assert "JSON.parse(e.data).payload" in serve.PAGE
        assert "addFrame(JSON.parse(e.data))" in serve.PAGE
        last = max(int(line[4:]) for line in text.splitlines() if line.startswith("id: "))
        status, _headers, replay = _get(server, f"/events?id={created['id']}", {"Last-Event-ID": str(last - 1)})
        assert status == 200 and f"id: {last}" in replay.decode()
    finally:
        server.shutdown(); server.server_close()


@pytest.mark.parametrize(("source", "sensitive_parts"), [
    (
        "rtsp://host-user:host-pass@camera.internal.example:8554/secure/live?token=hostname-token",
        ("host-user", "host-pass", "camera.internal.example", "8554", "/secure/live", "hostname-token"),
    ),
    (
        "rtsp://ipv4-user:ipv4-pass@10.24.7.9:9554/ipv4/live?token=ipv4-token",
        ("ipv4-user", "ipv4-pass", "10.24.7.9", "9554", "/ipv4/live", "ipv4-token"),
    ),
    (
        "rtsp://ipv6-user:ipv6-pass@[fd00:1234:5678::9]:10554/ipv6/live?token=ipv6-token",
        ("ipv6-user", "ipv6-pass", "fd00:1234:5678::9", "10554", "/ipv6/live", "ipv6-token"),
    ),
])
def test_rtsp_http_sse_and_status_never_echo_authority(
        manager, monkeypatch, source, sensitive_parts):
    manager.retention_seconds = 60

    def fake_rtsp_process(src, out_dir, *, event_sink, **_kwargs):
        assert src == source
        frames = os.path.join(out_dir, "frames")
        os.makedirs(frames, exist_ok=True)
        with open(os.path.join(frames, "rtsp_00000_00001.jpg"), "wb") as frame_file:
            frame_file.write(b"jpeg")
        event_sink(JOB_STARTED, {"source_kind": "rtsp"})
        event_sink("stream_started", {"transport": "tcp", "attempt": 1})
        event_sink("frame_kept", {
            "artifact": "frames/rtsp_00000_00001.jpg",
            "selection_reason": "rtsp_sample",
            "timestamp_seconds": 0.1,
        })
        event_sink(JOB_DONE, {"frame_count": 1, "extracted_frames": 1})
        return SimpleNamespace(frames_dir=frames, video="", frame_count=1)

    monkeypatch.setattr(serve, "process", fake_rtsp_process)
    monkeypatch.setattr(serve, "write_viewer", lambda *_args: "viewer.html")
    server = _server()
    try:
        status, created = _post(server, "/run", {
            "src": source,
            "opts": {"grid": False, "transcribe": False},
        })
        assert status == 200
        status, _headers, body = _get(server, f"/events?id={created['id']}")
        text = body.decode()
        assert status == 200
        assert "event: stream_started" in text
        assert "event: frame_kept" in text
        assert "event: job_done" in text
        status, _headers, status_body = _get(server, f"/status?id={created['id']}")
        assert status == 200
        public_http = json.dumps(created) + text + status_body.decode()
        for sensitive in (source, *sensitive_parts):
            assert sensitive not in public_http
        payloads = [json.loads(line[6:]) for line in text.splitlines() if line.startswith("data: ")]
        started = next(payload for payload in payloads if payload["type"] == JOB_STARTED)
        assert started["payload"] == {"source_kind": "rtsp"}
    finally:
        server.shutdown(); server.server_close()


def shutil_which(name):
    from shutil import which
    return which(name)


def _process_exists(pid):
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True, check=False,
        )
        return str(pid) in result.stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True
