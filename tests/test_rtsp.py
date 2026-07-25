"""M3 RTSP intake security and bounded-capture regressions."""
import json
import os
from pathlib import Path
import stat
import subprocess

import pytest

from claude_real_video.core import ProcessingCancelled
from claude_real_video.job_events import JOB_STARTED, JobEventBus
from claude_real_video.rtsp import (
    RtspCaptureError,
    RtspLimits,
    RtspSource,
    capture_rtsp_frames,
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
        RtspLimits(max_frames_per_minute=0)
    with pytest.raises(ValueError):
        RtspLimits(max_retained_frames=10_001)


def _fixture_source():
    username = "fixture-user"
    password = "fixture-pass"
    return (
        f"rtsp://{username}:{password}@127.0.0.1:8554/live?token=fixture-token",
        username,
        password,
    )
