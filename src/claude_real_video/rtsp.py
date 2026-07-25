"""Secure, bounded RTSP frame intake built on the M1/M2 event contracts."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field, replace
import hashlib
import math
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
import time
from typing import Any, Callable, Iterator
from urllib.parse import urlsplit

from .job_events import (
    STREAM_DONE,
    STREAM_ERROR,
    STREAM_RECONNECT,
    STREAM_STARTED,
    STREAM_TIMEOUT,
)
from .stream_windows import WindowEventProducer


CommandRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]


class RtspCaptureError(RuntimeError):
    """A stable RTSP failure code that never contains source or process details."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class RtspLimits:
    """Hard limits for one bounded RTSP capture attempt."""

    max_runtime_seconds: float = 30.0
    chunk_seconds: float = 5.0
    read_timeout_seconds: float = 5.0
    max_frames_per_minute: int = 60
    max_retained_frames: int = 120

    def __post_init__(self) -> None:
        if (not math.isfinite(self.max_runtime_seconds) or
                not 0 < self.max_runtime_seconds <= 3_600):
            raise ValueError("max_runtime_seconds must be in (0, 3600]")
        if (not math.isfinite(self.chunk_seconds) or
                not 0 < self.chunk_seconds <= 60):
            raise ValueError("chunk_seconds must be in (0, 60]")
        if (not math.isfinite(self.read_timeout_seconds) or
                not 0 < self.read_timeout_seconds <= 300):
            raise ValueError("read_timeout_seconds must be in (0, 300]")
        if not 0 < self.max_frames_per_minute <= 3_600:
            raise ValueError("max_frames_per_minute must be in [1, 3600]")
        if not 0 < self.max_retained_frames <= 10_000:
            raise ValueError("max_retained_frames must be in [1, 10000]")


@dataclass(frozen=True)
class RtspReconnectPolicy:
    """Deterministic reconnect policy for retryable transport failures."""

    max_reconnects: int = 2
    backoff_seconds: float = 0.5

    def __post_init__(self) -> None:
        if not 0 <= self.max_reconnects <= 10:
            raise ValueError("max_reconnects must be in [0, 10]")
        if (not math.isfinite(self.backoff_seconds) or
                not 0 <= self.backoff_seconds <= 60):
            raise ValueError("backoff_seconds must be in [0, 60]")


@dataclass(frozen=True, repr=False)
class RtspSource:
    """Parsed RTSP source whose repr never discloses credentials or path tokens."""

    _url: str = field(repr=False)
    redacted_url: str

    @classmethod
    def parse(cls, value: str) -> "RtspSource":
        if not isinstance(value, str) or not value:
            raise ValueError("RTSP source is required")
        if any(ord(char) < 32 or char in "'\\" for char in value):
            raise ValueError("RTSP source contains unsafe characters")
        parts = urlsplit(value)
        if parts.scheme.lower() != "rtsp" or not parts.hostname:
            raise ValueError("source must be an rtsp:// URL with a host")
        if parts.fragment:
            raise ValueError("RTSP source fragments are not supported")
        try:
            parts.port
        except ValueError as exc:
            raise ValueError("RTSP source has an invalid port") from exc
        return cls(value, _redacted_endpoint())

    def __repr__(self) -> str:
        return f"RtspSource(redacted_url={self.redacted_url!r})"

    @contextmanager
    def input_config(self, *, transport: str,
                     read_timeout_seconds: float) -> Iterator[str]:
        """Expose the source only through a short-lived 0600 ffconcat file."""
        if transport not in {"tcp", "udp"}:
            raise ValueError("RTSP transport must be tcp or udp")
        timeout_us = max(1, round(read_timeout_seconds * 1_000_000))
        directory = tempfile.mkdtemp(prefix="crv-rtsp-")
        os.chmod(directory, stat.S_IRWXU)
        path = os.path.join(directory, "input.ffconcat")
        descriptor = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            stat.S_IRUSR | stat.S_IWUSR,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as config:
                config.write("ffconcat version 1.0\n")
                config.write(f"file '{self._url}'\n")
                config.write(f"option rtsp_transport {transport}\n")
                config.write(f"option timeout {timeout_us}\n")
            yield path
        finally:
            shutil.rmtree(directory, ignore_errors=True)


def capture_rtsp_frames(producer: WindowEventProducer, source_url: str,
                        out_dir: str, *, command_runner: CommandRunner,
                        transport: str = "tcp",
                        limits: RtspLimits | None = None,
                        epoch_index: int = 0,
                        epoch_start_ms: int = 0) -> int:
    """Capture one bounded frame-only RTSP epoch and publish M1/M2 events."""
    return _capture_rtsp_epoch(
        producer, source_url, out_dir,
        command_runner=command_runner,
        transport=transport,
        limits=limits or RtspLimits(),
        epoch_index=epoch_index,
        epoch_start_ms=epoch_start_ms,
        emit_started=True,
        emit_done=True,
    )


def stream_rtsp_frames(producer: WindowEventProducer, source_url: str,
                       out_dir: str, *, command_runner: CommandRunner,
                       transport: str = "tcp",
                       limits: RtspLimits | None = None,
                       reconnect: RtspReconnectPolicy | None = None,
                       sleep: Callable[[float], None] = time.sleep,
                       monotonic: Callable[[], float] = time.monotonic) -> int:
    """Process bounded RTSP chunks with finite retries and monotonic epochs."""
    limits = limits or RtspLimits()
    reconnect = reconnect or RtspReconnectPolicy()
    RtspSource.parse(source_url)
    if transport not in {"tcp", "udp"}:
        raise ValueError("RTSP transport must be tcp or udp")
    producer.event_sink(STREAM_STARTED, {"transport": transport, "attempt": 1})

    deadline = monotonic() + limits.max_runtime_seconds
    media_runtime_remaining = limits.max_runtime_seconds
    frames_remaining = limits.max_retained_frames
    epoch_index = 0
    epoch_start_ms = 0
    reconnects = 0
    total_frames = 0
    while media_runtime_remaining > 0 and frames_remaining > 0:
        wall_seconds_remaining = deadline - monotonic()
        if wall_seconds_remaining <= 0:
            if total_frames == 0:
                producer.event_sink(STREAM_TIMEOUT, {"code": "stream_timeout"})
                raise RtspCaptureError("stream_timeout")
            break
        chunk_seconds = min(
            limits.chunk_seconds,
            media_runtime_remaining,
            wall_seconds_remaining,
        )
        chunk_limits = replace(
            limits,
            max_runtime_seconds=chunk_seconds,
            read_timeout_seconds=min(
                limits.read_timeout_seconds, wall_seconds_remaining,
            ),
            max_retained_frames=frames_remaining,
        )
        try:
            captured = _capture_rtsp_epoch(
                producer, source_url, out_dir,
                command_runner=command_runner,
                transport=transport,
                limits=chunk_limits,
                epoch_index=epoch_index,
                epoch_start_ms=epoch_start_ms,
                emit_started=False,
                emit_done=False,
            )
        except RtspCaptureError as exc:
            if exc.code not in _RETRYABLE_CODES or reconnects >= reconnect.max_reconnects:
                raise
            wall_seconds_remaining = max(0.0, deadline - monotonic())
            if wall_seconds_remaining <= reconnect.backoff_seconds:
                if total_frames:
                    break
                raise
            reconnects += 1
            producer.event_sink(STREAM_RECONNECT, {
                "attempt": reconnects + 1,
                "code": exc.code,
            })
            producer.reset_epoch(epoch_start_ms)
            if reconnect.backoff_seconds:
                sleep(reconnect.backoff_seconds)
            continue

        total_frames += captured
        frames_remaining -= captured
        media_runtime_remaining -= chunk_seconds
        epoch_start_ms += max(1, round(chunk_seconds * 1_000))
        epoch_index += 1

    producer.finish()
    producer.event_sink(STREAM_DONE, {"frame_count": total_frames})
    return total_frames


def process_rtsp(source_url: str, out_dir: str, *,
                 event_sink: Callable[[str, dict[str, Any]], None] | None = None,
                 process_controller: Any = None,
                 cancel_check: Callable[[], bool] | None = None,
                 overwrite: bool = False,
                 transport: str = "tcp",
                 limits: RtspLimits | None = None,
                 reconnect: RtspReconnectPolicy | None = None):
    """Run the frame-only M3 pipeline without persisting the RTSP source URL."""
    from .core import (
        ProcessController,
        ProcessingCancelled,
        Result,
        _prepare_out_dir,
        write_frames_json,
    )

    source = RtspSource.parse(source_url)
    limits = limits or RtspLimits()
    reconnect = reconnect or RtspReconnectPolicy()
    _prepare_out_dir(out_dir, overwrite)
    frames_dir = os.path.join(out_dir, "frames")
    records: list[dict[str, Any]] = []

    def safe_sink(event_type: str, data: dict[str, Any]) -> None:
        if event_type == "frame_kept" and data.get("artifact"):
            records.append({
                "name": os.path.basename(str(data["artifact"])),
                "kept": True,
                "t": float(data.get("timestamp_seconds") or 0),
                "via": "rtsp_sample",
            })
        if event_sink is not None:
            try:
                event_sink(event_type, dict(data))
            except Exception:
                pass

    if cancel_check is not None and cancel_check():
        raise ProcessingCancelled("job cancelled")
    controller = process_controller or ProcessController()
    producer = WindowEventProducer(safe_sink)
    extracted = stream_rtsp_frames(
        producer, source_url, frames_dir,
        command_runner=controller.run,
        transport=transport,
        limits=limits,
        reconnect=reconnect,
    )
    if cancel_check is not None and cancel_check():
        raise ProcessingCancelled("job cancelled")

    frames_json = write_frames_json(out_dir, records)
    manifest = os.path.join(out_dir, "MANIFEST.txt")
    transcript_note = "(skipped: RTSP M3 is frame-only)"
    with open(manifest, "w", encoding="utf-8") as output:
        output.write(
            f"source: {source.redacted_url}\n"
            f"mode: bounded RTSP frame-only intake ({transport})\n"
            f"duration budget: {limits.max_runtime_seconds:.3f}s\n"
            f"frames: {len(records)} kept from {extracted} sampled\n"
            f"frames dir: {frames_dir}\n"
            f"transcript: {transcript_note}\n"
        )
        if frames_json:
            output.write("frame timestamps: frames.json\n")
    return Result(
        out_dir=out_dir,
        video="",
        duration=max(1, round(limits.max_runtime_seconds)),
        frames_dir=frames_dir,
        frame_count=len(records),
        extracted_frames=extracted,
        transcript_path=None,
        manifest_path=manifest,
        transcript_note=transcript_note,
        frames_json_path=frames_json,
    )


def _capture_rtsp_epoch(producer: WindowEventProducer, source_url: str,
                        out_dir: str, *, command_runner: CommandRunner,
                        transport: str, limits: RtspLimits,
                        epoch_index: int, epoch_start_ms: int,
                        emit_started: bool, emit_done: bool) -> int:
    source = RtspSource.parse(source_url)
    if transport not in {"tcp", "udp"}:
        raise ValueError("RTSP transport must be tcp or udp")
    if isinstance(epoch_index, bool) or not isinstance(epoch_index, int) or epoch_index < 0:
        raise ValueError("epoch_index must be a non-negative integer")
    if isinstance(epoch_start_ms, bool) or not isinstance(epoch_start_ms, int) or epoch_start_ms < 0:
        raise ValueError("epoch_start_ms must be a non-negative integer")

    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    for stale in root.glob(f"rtsp_{epoch_index:05d}_*.jpg"):
        stale.unlink()
    pattern = root / f"rtsp_{epoch_index:05d}_%05d.jpg"
    sample_fps = limits.max_frames_per_minute / 60.0
    if emit_started:
        producer.event_sink(STREAM_STARTED, {
            "transport": transport,
            "attempt": epoch_index + 1,
        })

    with source.input_config(
        transport=transport,
        read_timeout_seconds=limits.read_timeout_seconds,
    ) as config_path:
        command = _capture_command(config_path, pattern, sample_fps, limits)
        result = command_runner(command)

    if result.returncode != 0:
        for partial in root.glob(f"rtsp_{epoch_index:05d}_*.jpg"):
            partial.unlink()
        code = _classify_failure(result.stderr)
        event_type = STREAM_TIMEOUT if code == "stream_timeout" else STREAM_ERROR
        producer.event_sink(event_type, {"code": code})
        raise RtspCaptureError(code)

    paths = sorted(root.glob(f"rtsp_{epoch_index:05d}_*.jpg"))
    if not paths:
        producer.event_sink(STREAM_TIMEOUT, {"code": "stream_no_frames"})
        raise RtspCaptureError("stream_no_frames")
    local_times = _showinfo_times(result.stderr)
    if len(local_times) != len(paths):
        local_times = [index / sample_fps for index in range(len(paths))]
    epoch_duration_ms = max(1, round(limits.max_runtime_seconds * 1_000))
    for path, local_seconds in zip(paths[:limits.max_retained_frames], local_times):
        media_time_ms = epoch_start_ms + min(
            epoch_duration_ms - 1,
            max(0, round(local_seconds * 1_000)),
        )
        with open(path, "rb") as image:
            signature = hashlib.blake2b(image.read(), digest_size=16).digest()
        producer.frame(signature, media_time_ms, {
            "artifact": os.path.relpath(path, root).replace(os.sep, "/"),
            "selection_reason": "rtsp_sample",
        })
    producer.finish()
    if emit_done:
        producer.event_sink(STREAM_DONE, {"frame_count": len(paths)})
    return len(paths)


def _capture_command(config_path: str, pattern: Path, sample_fps: float,
                     limits: RtspLimits) -> list[str]:
    timeout_us = max(1, round(limits.read_timeout_seconds * 1_000_000))
    return [
        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "info", "-y",
        "-rw_timeout", str(timeout_us),
        "-protocol_whitelist", "file,rtsp,tcp,udp",
        "-f", "concat", "-safe", "0", "-i", config_path,
        "-t", f"{limits.max_runtime_seconds:.3f}",
        "-an", "-sn", "-dn",
        "-vf", f"fps={sample_fps:.6f},showinfo,scale=640:-1",
        "-frames:v", str(limits.max_retained_frames),
        "-vsync", "vfr", str(pattern),
    ]


def _classify_failure(stderr: str) -> str:
    text = (stderr or "").lower()
    if any(token in text for token in (
        "401 unauthorized", "authentication failed", "method describe failed: 401",
    )):
        return "rtsp_auth_failed"
    if any(token in text for token in ("timed out", "timeout", "operation timed out")):
        return "stream_timeout"
    if any(token in text for token in (
        "connection refused", "no route to host", "network is unreachable",
        "name or service not known",
    )):
        return "stream_unreachable"
    if any(token in text for token in ("unsupported codec", "decoder not found", "unknown codec")):
        return "unsupported_codec"
    return "stream_failed"


def _showinfo_times(stderr: str) -> list[float]:
    return [max(0.0, float(value)) for value in re.findall(
        r"pts_time:\s*(-?[0-9]+(?:\.[0-9]+)?)", stderr or "",
    )]


def _redacted_endpoint() -> str:
    """Return the only RTSP source label allowed on public/persisted surfaces."""
    return "rtsp://<redacted>"


_RETRYABLE_CODES = frozenset({
    "stream_timeout", "stream_unreachable", "stream_failed", "stream_no_frames",
})
