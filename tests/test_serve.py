"""HTTP regressions for the local realtime event and artifact endpoints."""

from __future__ import annotations

import http.client
import json
import threading
from types import SimpleNamespace

from claude_real_video import serve
from claude_real_video.job_events import JOB_DONE, JOB_LOG, JOB_STARTED, SOURCE_READY, JobEventBus


def _server():
    server = serve.http.server.ThreadingHTTPServer(("127.0.0.1", 0), serve._Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _get(server, path, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
    conn.request("GET", path, headers=headers or {})
    response = conn.getresponse()
    body = response.read()
    conn.close()
    return response.status, dict(response.getheaders()), body


def test_sse_replays_from_since_and_last_event_id(tmp_path):
    jid = "events-job"
    bus = JobEventBus(clock=lambda: 123.0)
    serve.JOBS[jid] = {"state": "done", "log": "", "out_dir": str(tmp_path), "bus": bus}
    bus.emit(jid, JOB_STARTED, {"source_kind": "file"})
    bus.emit(jid, SOURCE_READY, {"artifact": "source.mp4"})
    bus.emit(jid, JOB_DONE, {"frame_count": 1})
    server, _thread = _server()
    try:
        status, headers, body = _get(server, f"/events?id={jid}&since=1")
        text = body.decode("utf-8")
        assert status == 200
        assert headers["Content-Type"].startswith("text/event-stream")
        assert "id: 2" in text and "event: source_ready" in text
        assert "id: 3" in text and "event: job_done" in text
        assert "id: 1" not in text

        status, _headers, body = _get(server, f"/events?id={jid}", {"Last-Event-ID": "2"})
        assert status == 200
        assert "id: 3" in body.decode("utf-8")
    finally:
        server.shutdown()
        server.server_close()
        serve.JOBS.pop(jid, None)


def test_artifact_endpoint_stays_inside_job_output(tmp_path):
    jid = "artifact-job"
    frames = tmp_path / "frames"
    frames.mkdir()
    image = frames / "frame_001.jpg"
    image.write_bytes(b"jpeg-bytes")
    secret = tmp_path.parent / "secret.txt"
    secret.write_text("not for this job", encoding="utf-8")
    serve.JOBS[jid] = {"state": "done", "log": "", "out_dir": str(tmp_path), "bus": JobEventBus()}
    server, _thread = _server()
    try:
        status, headers, body = _get(server, f"/artifacts?id={jid}&path=frames/frame_001.jpg")
        assert status == 200
        assert headers["Content-Type"].startswith("image/jpeg")
        assert body == b"jpeg-bytes"

        status, _headers, body = _get(server, f"/artifacts?id={jid}&path=../secret.txt")
        assert status == 404
        assert json.loads(body) == {"error": "artifact not found"}
    finally:
        server.shutdown()
        server.server_close()
        serve.JOBS.pop(jid, None)


def test_web_runner_routes_core_events_to_its_job_bus(tmp_path, monkeypatch):
    jid = "run-job"
    bus = JobEventBus(clock=lambda: 123.0)
    serve.JOBS[jid] = {"state": "running", "log": "", "out_dir": str(tmp_path), "bus": bus}

    def fake_process(_src, out_dir, *, event_sink, **_kwargs):
        event_sink(JOB_STARTED, {"source_kind": "file"})
        event_sink(JOB_LOG, {"message": "extracting"})
        event_sink(JOB_DONE, {"frame_count": 1})
        return SimpleNamespace(frames_dir=out_dir + "/frames", video=out_dir + "/source.mp4")

    monkeypatch.setattr(serve, "process", fake_process)
    monkeypatch.setattr(serve, "write_viewer", lambda *_args: "viewer.html")
    serve._run_job(jid, "input.mp4", {"grid": False, "transcribe": False})

    assert serve.JOBS[jid]["state"] == "done"
    assert "extracting" in serve.JOBS[jid]["log"]
    assert [event.type for event in bus.replay(jid)] == [JOB_STARTED, JOB_LOG, JOB_DONE]
    serve.JOBS.pop(jid, None)
