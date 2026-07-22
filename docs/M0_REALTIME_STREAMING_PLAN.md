# PROJ-017 M0 — Realtime Streaming Video Intake

Date: 2026-07-22
Owner: 欧游
Project: Claude-realtime-video

## 1. Repository Check

- Workspace: `/Volumes/GM7000/Projects/claude-realtime-video`
- Origin: `https://github.com/yetio/claude-realtime-video`
- Upstream source: `https://github.com/HUANGCHIHHUNGLeo/claude-real-video.git`
- Current branch: `master`
- Current HEAD: `e341616 docs: README accuracy — max-frames auto default, marketplace auto-update note, three dedup channels`
- Fork relation: `origin/master` and `upstream/master` point to the same HEAD (`e341616`).
- Push permission: `git push --dry-run origin HEAD:refs/heads/codex-push-check-proj017` succeeded, so origin push appears available.

## 2. Current Code Shape

This repo is a Python package named `claude-real-video` with a local-first batch pipeline.

- CLI entry: `src/claude_real_video/cli.py`
  - `crv` / `claude-real-video` parse source URL or file path and call `core.process(...)`.
  - Output is a completed folder containing frames, transcript, manifest, optional viewer/report/grid/audio.
- Core pipeline: `src/claude_real_video/core.py`
  - Downloads/copies source video.
  - Uses `ffmpeg`/`ffprobe` for frame extraction and media metadata.
  - Selects frames via scene score, FPS floor, adaptive mode, text anchors, and dedup.
  - Writes `frames.json`, `transcript.txt/json`, `MANIFEST.txt`, optional audio/report/export artifacts.
- Local web UI: `src/claude_real_video/serve.py`
  - Stdlib `ThreadingHTTPServer`.
  - `/run` starts a background CLI subprocess.
  - `/status` polls buffered logs.
  - `/open` opens the generated static `viewer.html`.
- Static viewer: `src/claude_real_video/viewer.py`
  - Writes a self-contained HTML file after the run finishes.
  - Displays video, keyframe grid, frame timestamps, and transcript.
- Tests: `tests/test_smoke.py`
  - Import and CLI help smoke tests.
  - `frames.json` end-to-end test with generated ffmpeg video.
  - Manifest transcript security-boundary regression.

Implication: current architecture is "analyze then view". There is no incremental output contract, no live job event stream, and no streaming viewer state model yet.

## 3. Target Interpretation

"实时流式视频的观看能力" should mean the local web UI and/or agent host can start consuming visual evidence before full video processing finishes.

M0 scope should not attempt full low-latency video conferencing. The closest product fit is:

- Feed video in chunks or a live-growing source.
- Emit keyframes, transcript segments, and status events incrementally.
- Let the viewer and agent watch a timeline update while processing continues.
- Preserve the existing local-first and "user chooses what to send to an LLM" privacy model.

## 4. Technical Options

### Option A — WebSocket/SSE Incremental Job Events

Add an in-process streaming API for `crv-web`.

- Backend:
  - Refactor `core.process(...)` into a generator-style pipeline or add callbacks.
  - Emit typed events: `job_started`, `source_ready`, `frame_kept`, `frame_dropped`, `transcript_segment`, `manifest_updated`, `job_done`, `job_error`.
  - Serve events via SSE first, WebSocket later if bidirectional control is needed.
- Frontend:
  - Replace `/status` log polling with event-driven updates.
  - Append keyframes and transcript lines as they arrive.
  - Keep current static viewer generation at job completion for compatibility.
- Pros:
  - Smallest change to current repo.
  - Fits stdlib server if using SSE over chunked HTTP.
  - Easy to test with fake event sinks.
- Cons:
  - Still depends on ffmpeg extraction cadence.
  - Not true browser MediaStream ingestion.

### Option B — Segment-Based Streaming Analyzer

Process a source as rolling time windows, e.g. 5-15 second chunks.

- Backend:
  - Split local file or URL download into segments.
  - Run current scene/dedup/transcript logic per segment.
  - Maintain cross-segment dedup state so repeated shots are not re-emitted.
- Pros:
  - Better fit for long videos and progressive download.
  - Can support "watch while downloading".
- Cons:
  - Needs careful timestamp normalization and dedup state across segment boundaries.
  - Whisper/faster-whisper streaming behavior is non-trivial; segment transcripts may need later reconciliation.

### Option C — Browser Capture / WebRTC Intake

Let users stream screen/camera/browser video into the local service.

- Backend:
  - Add a WebRTC or MediaRecorder upload path.
  - Convert short blobs into the same segment analyzer.
- Pros:
  - Enables actual live screen/camera watching.
  - Strong demo value.
- Cons:
  - Highest implementation complexity.
  - Requires browser JS capture UX and a non-stdlib dependency stack if using full WebRTC.
  - More security/permission surface.

### Option D — HLS/DASH Playlist Watcher

Watch a live or VOD HLS/DASH playlist and process new segments.

- Backend:
  - Accept `.m3u8` or stream URL.
  - Poll playlist and process new media segments.
- Pros:
  - Natural fit for many streaming sources.
  - Works without browser capture.
- Cons:
  - Source compatibility varies.
  - Still not enough for arbitrary local screen/video unless paired with a segment writer.

## 5. Recommendation

Start with Option A plus the minimum refactor needed for Option B later.

M1 should introduce an internal event contract without changing existing CLI output:

1. Add `core.process(..., event_sink=None)` or a new `process_stream(...)` wrapper.
2. Emit stable JSON-serializable events from the existing batch path.
3. Add a local SSE endpoint in `serve.py`.
4. Update the web page to render frames/transcript/logs incrementally.
5. Keep current `viewer.html` final artifact unchanged.

This gives a visible realtime experience quickly while avoiding premature WebRTC complexity. Once the event contract is stable, M2 can make the producer truly segment-based.

## 6. Milestone Draft

### M1 — Realtime Event Contract and Live Viewer

- Deliverables:
  - Typed event schema for job lifecycle, frame events, transcript events, and errors.
  - Event sink tests using generated tiny videos.
  - `crv-web` SSE endpoint.
  - Browser UI that updates keyframes/logs before job completion.
- Validation:
  - Existing `tests/test_smoke.py` still passes.
  - New tests assert event order and required fields.
  - Manual local run shows frames appearing before `job_done`.

### M2 — Segment/Window Processing

- Deliverables:
  - Segment runner for local files and progressive downloaded sources.
  - Cross-segment timestamp and dedup state.
  - Transcript segment reconciliation strategy.
- Validation:
  - Generated multi-scene video split into chunks emits monotonic timestamps.
  - Cross-boundary duplicate frame regression.

### M3 — Live Source Intake

- Deliverables:
  - HLS watcher or browser MediaRecorder upload path.
  - Live cancellation and backpressure controls.
  - Security notes for local-only streaming intake.
- Validation:
  - Long-running local live demo.
  - Negative tests for unsupported/invalid streams.

## 7. Review Gate

The realtime module is self-developed work on top of the fork. Before shipping it as a user-facing release, request architect review from 欧六. Review package should include:

- Event schema and API boundary.
- Security/privacy behavior for local server and uploaded/captured media.
- Backpressure/cancellation behavior.
- Test evidence and manual demo notes.

## 8. Immediate Next Step

Implement M1 as a narrow, backwards-compatible change:

- Add an event sink interface around existing pipeline milestones.
- Keep CLI behavior and output paths stable.
- Add SSE to web UI only after event tests lock the schema.
