# M3 RTSP Security Evidence

This document isolates the credential-handling evidence required for the M3
review. It does not claim release approval.

## Exposure Surfaces

### Credential lifecycle

- The user-provided RTSP URL is parsed in memory and is never written to the
  manifest, frames index, viewer, public event payload, or exception text.
- Authenticated sources are passed to ffmpeg through a temporary ffconcat file.
  On POSIX its directory is mode `0700` and the file is mode `0600`; on
  Windows the file inherits the current user's protected temporary-directory
  ACL because POSIX mode bits do not express Windows access control.
- The temporary directory is removed after success, ffmpeg failure, or
  `ProcessingCancelled`.

### Process argv

- The ffmpeg argv contains the temporary config path, transport-independent
  timeout values, output path and numeric limits.
- The RTSP URL, username, password and query string are absent from argv.
- An authenticated URL is also forbidden in the CLI positional argument,
  because that would expose it in the parent process argv and shell history.
  Authenticated CLI use must pass `--rtsp-source-file`; on POSIX it must be a
  mode `0600` file, while Windows relies on the file's user ACL. Unauthenticated
  `rtsp://` URLs may remain positional.
- `-nostdin` is always set. The caller must supply a `ProcessController.run`
  compatible runner so cancellation terminates and waits for the process group.

### Logs, events and errors

- ffmpeg stderr is captured in process memory only. It is classified into a
  stable code and is never copied into an event, manifest or raised error.
- Public stream events use an allowlist. Authentication, timeout, unreachable
  host and unsupported codec failures expose codes only.
- `RtspCaptureError.__str__` is the stable code; it does not contain the source,
  command or stderr.
- The local web server suppresses request logging and returns only the job ID
  from `/run`; the submitted source is not echoed.

### Persisted artifacts

- `MANIFEST.txt` stores only the constant `rtsp://<redacted>` label and numeric
  capture limits. It never retains hostnames, IP addresses, ports, paths,
  queries or userinfo.
- `frames.json` stores relative frame names and source-clock timestamps.
- CLI knowledge-base export uses `rtsp-stream-redacted` instead of the source
  URL when generating the note title and header.

## Hard Limits

- Total wall-clock capture budget: `rtsp_max_runtime_seconds`; successful
  chunks, failed attempts and reconnect backoff share one monotonic deadline.
- Per-command chunk runtime: `rtsp_chunk_seconds`.
- Socket read timeout: `rtsp_read_timeout_seconds`.
- Sampling ceiling: `rtsp_frames_per_minute`.
- Retained frame ceiling: `rtsp_max_retained_frames` across the whole stream.
- Reconnect ceiling: `rtsp_max_reconnects`; authentication and unsupported
  codec errors are non-retryable.
- Reconnect backoff waits on the caller's `ProcessController.cancel_event`, so
  web cancellation interrupts the wait instead of sleeping until the complete
  backoff expires.
- M1 still owns disk quota, subprocess cancellation, terminal arbitration,
  event replay bounds and cleanup.

## Automated Evidence

Run:

```bash
PYTHONPATH=src python3.12 -m pytest tests/test_rtsp.py tests/test_job_events.py tests/test_serve.py -q
```

The RTSP suite verifies separately:

- credentials are present only inside the live `0600` config fixture;
- the complete RTSP authority is absent from repr, events, manifest, status and
  knowledge-base exports across hostname, IPv4 and IPv6 sources;
- URL credentials are absent from argv and exceptions;
- temporary credentials are deleted on success, failure and cancellation;
- authenticated CLI sources are rejected from argv and accepted only from a
  private `0600` source file;
- TCP is the default and timeout/runtime/frame limits reach the command;
- timeout/unreachable failures retry within a hard ceiling;
- retries and backoff cannot extend the total wall-clock runtime budget;
- authentication and unsupported codec failures do not retry;
- reconnect calls the M2 epoch reset contract;
- static frames across chunks are deduplicated and drop events are aggregated;
- `core.process()` and the web worker retain the existing M1 lifecycle.

## Manual Negative Probe

The local no-credential probe below validates that the installed ffmpeg accepts
the temporary ffconcat input and returns a stable public code. It uses no real
camera or credential:

```text
source: rtsp://127.0.0.1:9/live
observed events: job_started -> stream_started -> stream_timeout
public code: stream_timeout
```

No real credential value should be added to this document, issue tracker,
terminal transcript, CI log or review message.
