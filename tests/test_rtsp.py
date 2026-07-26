"""M3 RTSP intake security and bounded-capture regressions."""
import json
import os
from pathlib import Path
import stat
import subprocess
import threading

import pytest

from claude_real_video.cli import _kb_source_label, _resolve_source
from claude_real_video.core import (
    ProcessController,
    ProcessingCancelled,
    process,
    save_to_kb,
)
from claude_real_video.job_events import JOB_STARTED, JobEventBus
from claude_real_video.rtsp import (
    RtspCaptureError,
    RtspLimits,
    RtspReconnectPolicy,
    RtspSource,
    capture_rtsp_frames,
    process_rtsp,
    stream_rtsp_frames,
)
from claude_real_video.stream_windows import WindowEventProducer


def test_rtsp_capture_keeps_credentials_out_of_argv_events_and_repr(tmp_path):
    source_url, username, password = _fixture_source()
    observed = {}

    def runner(command):
        config_path = command[command.index("-i") + 1]
        observed["command"] = list(command)
        observed["config_path"] = config_path
        observed["config"] = Path(config_path).read_text(encoding="utf-8")
        observed["mode"] = stat.S_IMODE(os.stat(config_path).st_mode)
        pattern = next(value for value in command if "%05d.jpg" in value)
        Path(pattern.replace("%05d", "00001")).write_bytes(b"fixture-frame")
        return subprocess.CompletedProcess(command, 0, "", "pts_time:0.250")

    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("rtsp-job", JOB_STARTED, {"source_kind": "rtsp"})
    producer = WindowEventProducer(bus.event_sink("rtsp-job"), dedup_ttl_ms=1)
    count = capture_rtsp_frames(
        producer, source_url, str(tmp_path / "frames"),
        command_runner=runner,
        limits=RtspLimits(
            max_runtime_seconds=2,
            read_timeout_seconds=1,
            max_frames_per_minute=60,
            max_retained_frames=2,
        ),
    )

    assert count == 1
    command_text = " ".join(observed["command"])
    assert source_url not in command_text
    assert username not in command_text
    assert password not in command_text
    assert "-nostdin" in observed["command"]
    assert observed["command"][observed["command"].index("-rw_timeout") + 1] == "1000000"
    assert observed["command"][observed["command"].index("-frames:v") + 1] == "2"
    assert observed["command"][observed["command"].index("-t") + 1] == "2.000"
    if os.name == "posix":
        assert observed["mode"] == 0o600
    assert source_url in observed["config"]
    assert "option rtsp_transport tcp" in observed["config"]
    assert "option timeout 1000000" in observed["config"]
    assert not os.path.exists(observed["config_path"])
    assert username not in repr(RtspSource.parse(source_url))
    assert password not in repr(RtspSource.parse(source_url))

    events = bus.replay("rtsp-job")
    assert [event.type for event in events] == [
        "job_started", "stream_started", "frame_kept", "stream_done",
    ]
    serialized = json.dumps([event.to_dict() for event in events])
    assert source_url not in serialized
    assert username not in serialized
    assert password not in serialized
    assert events[0].payload == {"source_kind": "rtsp"}
    assert events[1].payload == {"transport": "tcp", "attempt": 1}


@pytest.mark.parametrize(("stderr", "expected_code", "expected_event"), [
    ("401 Unauthorized", "rtsp_auth_failed", "stream_error"),
    ("Connection timed out", "stream_timeout", "stream_timeout"),
    ("Connection refused", "stream_unreachable", "stream_error"),
    ("Decoder not found: fixture", "unsupported_codec", "stream_error"),
])
def test_rtsp_failures_expose_stable_codes_only(tmp_path, stderr, expected_code, expected_event):
    source_url, username, password = _fixture_source()

    def runner(command):
        return subprocess.CompletedProcess(
            command, 1, "", f"{stderr}: {source_url}",
        )

    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("rtsp-error", JOB_STARTED, {"source_kind": "rtsp"})
    producer = WindowEventProducer(bus.event_sink("rtsp-error"))
    with pytest.raises(RtspCaptureError, match=f"^{expected_code}$"):
        capture_rtsp_frames(
            producer, source_url, str(tmp_path), command_runner=runner,
        )

    event = bus.replay("rtsp-error")[-1]
    assert event.type == expected_event
    assert event.payload == {"code": expected_code}
    serialized = json.dumps(event.to_dict())
    assert source_url not in serialized
    assert username not in serialized
    assert password not in serialized


def test_rtsp_config_is_removed_when_controlled_runner_cancels(tmp_path):
    source_url, _username, _password = _fixture_source()
    observed = {}

    def runner(command):
        observed["config_path"] = command[command.index("-i") + 1]
        assert os.path.isfile(observed["config_path"])
        raise ProcessingCancelled("job cancelled")

    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("rtsp-cancel", JOB_STARTED, {"source_kind": "rtsp"})
    producer = WindowEventProducer(bus.event_sink("rtsp-cancel"))
    with pytest.raises(ProcessingCancelled, match="job cancelled"):
        capture_rtsp_frames(
            producer, source_url, str(tmp_path), command_runner=runner,
        )
    assert not os.path.exists(observed["config_path"])


def test_rtsp_contract_rejects_unsafe_sources_and_unbounded_limits():
    for value in ("http://camera/live", "rtsp:///missing-host", "rtsp://host/live#fragment"):
        with pytest.raises(ValueError):
            RtspSource.parse(value)
    with pytest.raises(ValueError, match="unsafe characters"):
        RtspSource.parse("rtsp://host/live\noption timeout 0")
    with pytest.raises(ValueError):
        RtspLimits(max_runtime_seconds=0)
    with pytest.raises(ValueError):
        RtspLimits(chunk_seconds=0)
    with pytest.raises(ValueError):
        RtspLimits(max_frames_per_minute=0)
    with pytest.raises(ValueError):
        RtspLimits(max_retained_frames=10_001)
    with pytest.raises(ValueError):
        RtspReconnectPolicy(max_reconnects=11)


def test_rtsp_stream_reconnects_retryable_failure_and_resets_epoch(tmp_path):
    source_url, username, password = _fixture_source()
    calls = []
    sleeps = []

    def runner(command):
        calls.append(list(command))
        if len(calls) == 1:
            return subprocess.CompletedProcess(command, 1, "", "Connection timed out")
        pattern = next(value for value in command if "%05d.jpg" in value)
        Path(pattern.replace("%05d", "00001")).write_bytes(b"reconnected-frame")
        return subprocess.CompletedProcess(command, 0, "", "pts_time:0.100")

    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("rtsp-reconnect", JOB_STARTED, {"source_kind": "rtsp"})
    producer = WindowEventProducer(bus.event_sink("rtsp-reconnect"), dedup_ttl_ms=5_000)
    count = stream_rtsp_frames(
        producer, source_url, str(tmp_path), command_runner=runner,
        limits=RtspLimits(
            max_runtime_seconds=1, chunk_seconds=1, read_timeout_seconds=1,
            max_frames_per_minute=60, max_retained_frames=1,
        ),
        reconnect=RtspReconnectPolicy(max_reconnects=1, backoff_seconds=0.25),
        sleep=sleeps.append,
    )

    assert count == 1
    assert len(calls) == 2
    assert sleeps == [0.25]
    assert producer.runner.clock.watermark_ms == 100
    events = bus.replay("rtsp-reconnect")
    assert [event.type for event in events] == [
        "job_started", "stream_started", "stream_timeout", "stream_reconnect",
        "frame_kept", "stream_done",
    ]
    serialized = json.dumps([event.to_dict() for event in events])
    assert username not in serialized
    assert password not in serialized


def test_rtsp_retries_and_backoff_share_the_total_wall_clock_budget(tmp_path):
    source_url, _username, _password = _fixture_source()
    now = [0.0]
    calls = []

    def runner(command):
        calls.append(list(command))
        now[0] += float(command[command.index("-t") + 1])
        return subprocess.CompletedProcess(command, 1, "", "Connection timed out")

    def sleep(seconds):
        now[0] += seconds

    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("rtsp-deadline", JOB_STARTED, {"source_kind": "rtsp"})
    producer = WindowEventProducer(bus.event_sink("rtsp-deadline"))
    with pytest.raises(RtspCaptureError, match="^stream_timeout$"):
        stream_rtsp_frames(
            producer, source_url, str(tmp_path), command_runner=runner,
            limits=RtspLimits(
                max_runtime_seconds=2, chunk_seconds=1,
                read_timeout_seconds=5, max_frames_per_minute=60,
                max_retained_frames=10,
            ),
            reconnect=RtspReconnectPolicy(max_reconnects=5, backoff_seconds=0.5),
            sleep=sleep,
            monotonic=lambda: now[0],
        )

    assert len(calls) == 2
    assert [command[command.index("-t") + 1] for command in calls] == ["1.000", "0.500"]
    assert [command[command.index("-rw_timeout") + 1] for command in calls] == [
        "2000000", "500000",
    ]
    assert now[0] == 2.0
    assert [event.type for event in bus.replay("rtsp-deadline")] == [
        "job_started", "stream_started", "stream_timeout", "stream_reconnect",
        "stream_timeout",
    ]


def test_process_rtsp_cancel_interrupts_reconnect_backoff(tmp_path):
    source_url, _username, _password = _fixture_source()
    reconnect_started = threading.Event()
    outcome = []

    class FailingController(ProcessController):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def run(self, command):
            self.calls += 1
            return subprocess.CompletedProcess(
                command, 1, "", "Connection timed out",
            )

    controller = FailingController()

    def event_sink(event_type, _data):
        if event_type == "stream_reconnect":
            reconnect_started.set()

    def worker():
        try:
            process_rtsp(
                source_url,
                str(tmp_path / "analysis"),
                event_sink=event_sink,
                process_controller=controller,
                limits=RtspLimits(
                    max_runtime_seconds=60,
                    chunk_seconds=1,
                    read_timeout_seconds=1,
                    max_frames_per_minute=60,
                    max_retained_frames=2,
                ),
                reconnect=RtspReconnectPolicy(
                    max_reconnects=1,
                    backoff_seconds=30,
                ),
            )
        except ProcessingCancelled:
            outcome.append("cancelled")

    thread = threading.Thread(target=worker)
    thread.start()
    assert reconnect_started.wait(timeout=1), "RTSP retry did not enter backoff"
    controller.cancel()
    thread.join(timeout=1)

    assert outcome == ["cancelled"]
    assert controller.calls == 1
    assert not thread.is_alive(), "cancel waited for the full reconnect backoff"


def test_process_rtsp_cancel_check_interrupts_backoff_with_default_controller(
    tmp_path, monkeypatch,
):
    source_url, _username, _password = _fixture_source()
    capture_calls = []
    cancel_checks = []

    def fail_capture(_controller, command):
        capture_calls.append(command)
        return subprocess.CompletedProcess(
            command, 1, "", "Connection timed out",
        )

    def cancel_check():
        cancel_checks.append(True)
        return len(cancel_checks) >= 2

    monkeypatch.setattr(ProcessController, "run", fail_capture)
    with pytest.raises(ProcessingCancelled, match="^job cancelled$"):
        process_rtsp(
            source_url,
            str(tmp_path / "analysis"),
            cancel_check=cancel_check,
            limits=RtspLimits(
                max_runtime_seconds=60,
                chunk_seconds=1,
                read_timeout_seconds=1,
                max_frames_per_minute=60,
                max_retained_frames=2,
            ),
            reconnect=RtspReconnectPolicy(
                max_reconnects=1,
                backoff_seconds=30,
            ),
        )

    assert len(capture_calls) == 1
    assert len(cancel_checks) == 2


def test_rtsp_stream_does_not_retry_auth_or_unsupported_codec(tmp_path):
    source_url, _username, _password = _fixture_source()
    for stderr, expected in (
        ("401 Unauthorized", "rtsp_auth_failed"),
        ("Decoder not found", "unsupported_codec"),
    ):
        calls = []

        def runner(command):
            calls.append(command)
            return subprocess.CompletedProcess(command, 1, "", stderr)

        bus = JobEventBus(clock=lambda: 1.0)
        bus.emit(expected, JOB_STARTED, {"source_kind": "rtsp"})
        producer = WindowEventProducer(bus.event_sink(expected))
        with pytest.raises(RtspCaptureError, match=f"^{expected}$"):
            stream_rtsp_frames(
                producer, source_url, str(tmp_path / expected),
                command_runner=runner,
                reconnect=RtspReconnectPolicy(max_reconnects=3, backoff_seconds=0),
            )
        assert len(calls) == 1
        assert "stream_reconnect" not in [event.type for event in bus.replay(expected)]


def test_static_rtsp_chunks_remain_bounded_and_deduplicated(tmp_path):
    source_url, _username, _password = _fixture_source()
    calls = []

    def runner(command):
        calls.append(command)
        pattern = next(value for value in command if "%05d.jpg" in value)
        Path(pattern.replace("%05d", "00001")).write_bytes(b"static-camera-frame")
        return subprocess.CompletedProcess(command, 0, "", "pts_time:0.100")

    bus = JobEventBus(clock=lambda: 1.0)
    bus.emit("rtsp-static", JOB_STARTED, {"source_kind": "rtsp"})
    producer = WindowEventProducer(bus.event_sink("rtsp-static"), dedup_ttl_ms=5_000)
    count = stream_rtsp_frames(
        producer, source_url, str(tmp_path), command_runner=runner,
        limits=RtspLimits(
            max_runtime_seconds=3, chunk_seconds=1, read_timeout_seconds=1,
            max_frames_per_minute=60, max_retained_frames=3,
        ),
        reconnect=RtspReconnectPolicy(max_reconnects=0),
    )

    assert count == 3
    assert len(calls) == 3
    events = bus.replay("rtsp-static")
    assert [event.type for event in events] == [
        "job_started", "stream_started", "frame_kept", "frame_dropped", "stream_done",
    ]
    dropped = next(event for event in events if event.type == "frame_dropped")
    assert dropped.payload["count"] == 2
    assert len(list(tmp_path.glob("*.jpg"))) == 3


@pytest.mark.parametrize(("source_url", "sensitive_parts"), [
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
def test_core_process_redacts_entire_rtsp_authority_from_public_outputs(
        tmp_path, source_url, sensitive_parts):
    commands = []

    class Controller:
        def run(self, command):
            commands.append(list(command))
            pattern = next(value for value in command if "%05d.jpg" in value)
            Path(pattern.replace("%05d", "00001")).write_bytes(
                f"frame-{len(commands)}".encode(),
            )
            return subprocess.CompletedProcess(command, 0, "", "pts_time:0.100")

    events = []
    out = tmp_path / "analysis"
    result = process(
        source_url, str(out),
        event_sink=lambda event_type, data: events.append((event_type, data)),
        process_controller=Controller(),
        rtsp_max_runtime_seconds=2,
        rtsp_chunk_seconds=1,
        rtsp_read_timeout_seconds=1,
        rtsp_frames_per_minute=60,
        rtsp_max_retained_frames=2,
        rtsp_max_reconnects=0,
    )

    assert result.frame_count == 2
    assert result.extracted_frames == 2
    assert result.video == ""
    assert result.transcript_path is None
    assert os.path.isfile(result.frames_json_path)
    manifest = Path(result.manifest_path).read_text(encoding="utf-8")
    serialized = json.dumps(events)
    kb_path = save_to_kb(
        str(tmp_path / "kb"), result.manifest_path, _kb_source_label(source_url),
    )
    kb_export = Path(kb_path).read_text(encoding="utf-8")
    parsed_source = RtspSource.parse(source_url)
    public_outputs = (manifest, serialized, kb_export, repr(parsed_source), str(parsed_source))
    for sensitive in (source_url, *sensitive_parts):
        assert all(sensitive not in output for output in public_outputs)
        assert all(sensitive not in " ".join(command) for command in commands)
    assert events[0] == ("job_started", {"source_kind": "rtsp"})
    assert events[-1][0] == "job_done"
    assert "source: rtsp://<redacted>" in manifest
    assert parsed_source.redacted_url == "rtsp://<redacted>"
    assert _kb_source_label(source_url) == "rtsp-stream-redacted"


def test_cli_requires_private_file_for_authenticated_rtsp(tmp_path):
    source_url, _username, _password = _fixture_source()
    with pytest.raises(ValueError, match="must use --rtsp-source-file"):
        _resolve_source(source_url, None)

    source_file = tmp_path / "rtsp-source.txt"
    source_file.write_text(source_url, encoding="utf-8")
    if os.name == "posix":
        os.chmod(source_file, 0o644)
        with pytest.raises(ValueError, match="chmod 600"):
            _resolve_source(None, str(source_file))
        os.chmod(source_file, 0o600)
    assert _resolve_source(None, str(source_file)) == source_url
    assert _resolve_source("rtsp://127.0.0.1/live", None) == "rtsp://127.0.0.1/live"


def _fixture_source():
    username = "fixture-user"
    password = "fixture-pass"
    return (
        f"rtsp://{username}:{password}@127.0.0.1:8554/live?token=fixture-token",
        username,
        password,
    )
