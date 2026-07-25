# M3 RTSP Live Fixture

`tests/test_rtsp_live.py` is the reproducible positive-path fixture for M3.
It does not mock the capture runner and does not require a camera, account,
credential, container, or persistent RTSP service.

The test performs the following bounded flow:

1. Uses the installed `ffmpeg` to generate a three-second MPEG-TS test source.
2. Starts a loopback-only Python RTSP server on an ephemeral TCP port.
3. Completes OPTIONS, DESCRIBE, SETUP, and PLAY, then sends the source as
   interleaved RTP over the same TCP connection.
4. Calls the public `process()` RTSP path, which starts the product ffmpeg
   capture through `ProcessController` and writes sampled JPEG artifacts.
5. Verifies non-empty frames, the public event lifecycle, and that neither the
   manifest nor serialized events contain the raw source URL.

Run the fixture from the repository root:

```bash
PYTHONPATH=src python3 -m pytest -q tests/test_rtsp_live.py
```

Local development skips the fixture when `ffmpeg` is absent. CI explicitly
installs ffmpeg on Ubuntu, macOS and Windows, sets `CRV_REQUIRE_FFMPEG=1`, runs
`ffmpeg -version` as a fail-closed preflight, and executes this fixture before
the full suite. A missing dependency therefore fails the release gate instead
of becoming a green skip. The server binds to `127.0.0.1`, accepts one
connection, has no authentication surface, and is stopped in a `finally` path.
Production credential handling remains covered separately by
`docs/M3_RTSP_SECURITY_EVIDENCE.md` and `tests/test_rtsp.py`.
