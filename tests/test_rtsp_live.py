"""Real ffmpeg RTSP success-path test using a local bounded fixture server."""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import threading
import time

import pytest

from claude_real_video.core import process


@pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg not installed")
def test_local_rtsp_fixture_produces_real_frames(tmp_path):
    events = []
    with local_rtsp_fixture(tmp_path) as source_url:
        result = process(
            source_url,
            str(tmp_path / "analysis"),
            event_sink=lambda event_type, data: events.append((event_type, data)),
            rtsp_max_runtime_seconds=2,
            rtsp_chunk_seconds=2,
            rtsp_read_timeout_seconds=3,
            rtsp_frames_per_minute=120,
            rtsp_max_retained_frames=4,
            rtsp_max_reconnects=0,
        )

    frame_paths = sorted(Path(result.frames_dir).glob("*.jpg"))
    assert result.frame_count >= 1
    assert result.extracted_frames >= result.frame_count
    assert frame_paths and all(path.stat().st_size > 0 for path in frame_paths)
    assert [event_type for event_type, _data in events][0] == "job_started"
    assert "stream_started" in [event_type for event_type, _data in events]
    assert "frame_kept" in [event_type for event_type, _data in events]
    assert [event_type for event_type, _data in events][-1] == "job_done"
    assert source_url not in json.dumps(events)
    assert source_url not in Path(result.manifest_path).read_text(encoding="utf-8")


@contextmanager
def local_rtsp_fixture(tmp_path: Path):
    transport_stream = tmp_path / "fixture.ts"
    subprocess.run([
        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", "testsrc2=size=160x120:rate=10",
        "-t", "3", "-an", "-c:v", "mpeg2video", "-g", "10", "-bf", "0",
        "-f", "mpegts", str(transport_stream),
    ], check=True, capture_output=True)
    server = _RtspFixtureServer(transport_stream.read_bytes())
    server.start()
    try:
        yield server.url
    finally:
        server.close()


class _RtspFixtureServer:
    def __init__(self, transport_stream: bytes) -> None:
        if not transport_stream or len(transport_stream) % 188:
            raise ValueError("fixture transport stream must contain complete TS packets")
        self.transport_stream = transport_stream
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self._listener.settimeout(0.2)
        self.port = self._listener.getsockname()[1]
        self.url = f"rtsp://127.0.0.1:{self.port}/live"
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._error: BaseException | None = None

    def start(self) -> None:
        self._thread.start()
        if not self._ready.wait(timeout=2):
            raise RuntimeError("RTSP fixture did not start")

    def close(self) -> None:
        self._stop.set()
        self._listener.close()
        self._thread.join(timeout=3)
        if self._thread.is_alive():
            raise RuntimeError("RTSP fixture did not stop")
        if self._error is not None:
            raise RuntimeError("RTSP fixture failed") from self._error

    def _serve(self) -> None:
        self._ready.set()
        try:
            while not self._stop.is_set():
                try:
                    connection, _address = self._listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return
                with connection:
                    connection.settimeout(3)
                    self._serve_connection(connection)
                return
        except (BrokenPipeError, ConnectionResetError):
            return
        except BaseException as exc:
            self._error = exc

    def _serve_connection(self, connection: socket.socket) -> None:
        pending = b""
        while not self._stop.is_set():
            while b"\r\n\r\n" not in pending:
                chunk = connection.recv(4096)
                if not chunk:
                    return
                pending += chunk
            raw_request, pending = pending.split(b"\r\n\r\n", 1)
            lines = raw_request.decode("ascii").split("\r\n")
            method, request_url, _version = lines[0].split(" ", 2)
            headers = {
                key.lower(): value.strip()
                for key, value in (line.split(":", 1) for line in lines[1:] if ":" in line)
            }
            cseq = headers.get("cseq", "1")
            if method == "OPTIONS":
                self._respond(connection, cseq, {
                    "Public": "OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN",
                })
            elif method == "DESCRIBE":
                body = (
                    "v=0\r\n"
                    "o=- 0 0 IN IP4 127.0.0.1\r\n"
                    "s=claude-real-video fixture\r\n"
                    "c=IN IP4 127.0.0.1\r\n"
                    "t=0 0\r\n"
                    "a=control:*\r\n"
                    "a=range:npt=0-\r\n"
                    "m=video 0 RTP/AVP 33\r\n"
                    "a=control:trackID=0\r\n"
                ).encode("ascii")
                self._respond(connection, cseq, {
                    "Content-Base": f"{self.url}/",
                    "Content-Type": "application/sdp",
                }, body)
            elif method == "SETUP":
                transport = headers.get(
                    "transport", "RTP/AVP/TCP;unicast;interleaved=0-1",
                )
                self._respond(connection, cseq, {
                    "Session": "fixture-session;timeout=60",
                    "Transport": transport,
                })
            elif method == "PLAY":
                self._respond(connection, cseq, {
                    "Session": "fixture-session",
                    "Range": "npt=0.000-",
                    "RTP-Info": f"url={request_url}/trackID=0;seq=1;rtptime=0",
                })
                self._stream_rtp(connection)
                return
            elif method == "TEARDOWN":
                self._respond(connection, cseq, {"Session": "fixture-session"})
                return
            else:
                self._respond(connection, cseq, {}, status="501 Not Implemented")

    def _respond(self, connection: socket.socket, cseq: str,
                 headers: dict[str, str], body: bytes = b"",
                 status: str = "200 OK") -> None:
        response_headers = {
            "CSeq": cseq,
            "Server": "crv-test-fixture",
            **headers,
        }
        if body:
            response_headers["Content-Length"] = str(len(body))
        response = [f"RTSP/1.0 {status}"]
        response.extend(f"{key}: {value}" for key, value in response_headers.items())
        connection.sendall(("\r\n".join(response) + "\r\n\r\n").encode("ascii") + body)

    def _stream_rtp(self, connection: socket.socket) -> None:
        payload_size = 188 * 7
        payloads = [
            self.transport_stream[offset:offset + payload_size]
            for offset in range(0, len(self.transport_stream), payload_size)
        ]
        delay = 3.0 / len(payloads)
        timestamp_step = max(1, round(270_000 / len(payloads)))
        sequence = 1
        timestamp = 0
        ssrc = 0x43525631
        for payload in payloads:
            if self._stop.is_set():
                return
            rtp = struct.pack("!BBHII", 0x80, 33, sequence, timestamp, ssrc) + payload
            connection.sendall(b"$\x00" + struct.pack("!H", len(rtp)) + rtp)
            sequence = (sequence + 1) & 0xFFFF
            timestamp = (timestamp + timestamp_step) & 0xFFFFFFFF
            time.sleep(delay)
