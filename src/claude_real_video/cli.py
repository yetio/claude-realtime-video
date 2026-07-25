"""Command-line interface for claude-real-video."""
from __future__ import annotations

import argparse
import os
import stat
import sys
from urllib.parse import urlsplit

from .core import process


def main() -> None:
    # Windows consoles default to a legacy codepage (e.g. cp950 on zh-Hant),
    # which cannot encode the ✓/→/— glyphs below and crashes the CLI on exit
    # even though processing succeeded. Force UTF-8 so output is codepage-safe.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    ap = argparse.ArgumentParser(
        prog="claude-real-video",
        description="Let Claude (or any LLM) actually watch a video: scene-aware, "
                    "deduplicated frames + a transcript, from a URL or a local file.",
    )
    ap.add_argument("source", nargs="?", help="Video/unauthenticated RTSP URL or a local file path")
    ap.add_argument("-o", "--out", default="crv-out", help="Output directory (default: ./crv-out)")
    ap.add_argument("--overwrite", action="store_true",
                    help="Replace a previous analysis living in the output directory "
                         "(without this, a non-empty output dir is refused to avoid mixing videos)")
    ap.add_argument("--scene", type=float, default=0.30,
                    help="Scene-change sensitivity 0-1, lower = more frames (default: 0.30)")
    ap.add_argument("--fps-floor", "--min-frame-interval", dest="fps_floor", type=float, default=1.0,
                    help="SECONDS PER FRAME, not fps — guarantee at least one frame "
                         "every N seconds (default: 1.0; --min-frame-interval is an alias)")
    ap.add_argument("--max-frames", type=int, default=None,
                    help="Cap total frames (default: auto — scales with duration, "
                         "clamp(150, seconds*1.5, 600))")
    ap.add_argument("--adaptive", action="store_true",
                    help="Adaptive scene detection: catches slow morphs (2-3s squash/stretch, "
                         "gradual pans) a fixed threshold misses, by comparing each frame "
                         "against its rolling neighbourhood instead of a constant")
    ap.add_argument("--text-anchors", action="store_true",
                    help="Force extra frames at subtitle-cue timestamps (sidecar .srt/.vtt "
                         "or embedded track) — for videos where meaning changes faster than "
                         "pixels: burned-in captions, lecture slides, screen recordings. "
                         "At most one forced frame per second; scene detection unchanged")
    ap.add_argument("--export", choices=["llc"], default=None,
                    help="Also export the analysis for other tools. 'llc' writes a "
                         "lossless-cut project (highlights.llc): every detected scene "
                         "becomes a cut segment — open it, keep the highlights, delete "
                         "the rest (issue #10)")
    ap.add_argument("--speakers", action="store_true",
                    help="Label who spoke when: offline speaker diarization "
                         "(sherpa-onnx, no account/token needed — models auto-download "
                         "on first use). Transcript lines get [SPEAKER_00]-style prefixes; "
                         "needs: pip install 'claude-real-video[speakers]'")
    ap.add_argument("--lang", default="auto", help="Whisper language, e.g. en / zh / auto (default: auto)")
    ap.add_argument("--cookies", default=None,
                    help="Netscape cookie file for sites that need login (your own, authorised use only)")
    ap.add_argument("--cookies-from-browser", default=None, metavar="BROWSER",
                    help="read login cookies straight from your own browser — chrome, safari, "
                         "firefox or edge. For sites that need login (your own account only)")
    ap.add_argument("--no-transcribe", action="store_true", help="Skip audio transcription")
    ap.add_argument("--viewer", action="store_true",
                    help="also write viewer.html — browse the video, keyframes and "
                         "transcript in one local page (double-click to open)")
    ap.add_argument("--whisper-model", default="base",
                    choices=["tiny", "base", "small", "medium", "large", "turbo"],
                    help="Whisper model for transcription (default: base — fast; "
                         "pick turbo for large-v3 accuracy at ~8x large's speed, "
                         "medium/large also available — bigger models download more)")
    ap.add_argument("--dedup-threshold", type=float, default=8,
                    help="Percent of pixels that must change for a frame to count as new; "
                         "higher = fewer frames kept (default: 8)")
    ap.add_argument("--dedup-window", type=int, default=4,
                    help="Compare each frame against the last N kept frames, so a shot "
                         "the model already saw doesn't come back after a cutaway "
                         "(1 = classic consecutive-only, default: 4)")
    ap.add_argument("--report", action="store_true",
                    help="Keep dropped frames in ./dropped and write report.html "
                         "visualising every keep/drop decision, for tuning the threshold")
    ap.add_argument("--why", default=None,
                    help='Why you are watching, e.g. --why "find the pricing strategy" — '
                         "written into MANIFEST.txt so the model analyses with that lens "
                         "instead of producing a generic summary")
    ap.add_argument("--grid", action="store_true",
                    help="Also tile the kept frames into 3x3 contact sheets (./grids) — "
                         "consecutive frames side by side help the model follow motion "
                         "and progression instead of guessing between stills")
    ap.add_argument("--kb", default=None, metavar="DIR",
                    help="Also save the analysis as a dated markdown note into this "
                         "knowledge-base folder (e.g. your Obsidian vault)")
    ap.add_argument("--keep-audio", action="store_true",
                    help="Also save the full original soundtrack (music + speech) as audio.m4a, "
                         "for models that can listen to audio (Gemini, GPT-4o, ...)")
    ap.add_argument("--rtsp-transport", choices=["tcp", "udp"], default="tcp",
                    help="RTSP transport (default: tcp)")
    ap.add_argument("--rtsp-runtime", type=float, default=30.0, metavar="SECONDS",
                    help="RTSP total capture budget (default: 30s)")
    ap.add_argument("--rtsp-chunk", type=float, default=5.0, metavar="SECONDS",
                    help="RTSP capture chunk size (default: 5s)")
    ap.add_argument("--rtsp-read-timeout", type=float, default=5.0, metavar="SECONDS",
                    help="RTSP socket read timeout (default: 5s)")
    ap.add_argument("--rtsp-frames-per-minute", type=int, default=60,
                    help="RTSP sampling ceiling (default: 60/min)")
    ap.add_argument("--rtsp-max-frames", type=int, default=120,
                    help="RTSP retained artifact ceiling (default: 120)")
    ap.add_argument("--rtsp-reconnects", type=int, default=2,
                    help="RTSP retry ceiling for transient failures (default: 2)")
    ap.add_argument("--rtsp-source-file", metavar="FILE",
                    help="read an authenticated RTSP URL from a private 0600 file")
    args = ap.parse_args()

    try:
        source = _resolve_source(args.source, args.rtsp_source_file)
        r = process(
            source, args.out,
            scene=args.scene, adaptive=args.adaptive, text_anchors=args.text_anchors,
            fps_floor=args.fps_floor, max_frames=args.max_frames,
            lang=args.lang, cookies=args.cookies, cookies_from_browser=args.cookies_from_browser,
            do_transcribe=not args.no_transcribe,
            whisper_model=args.whisper_model, dedup_threshold=args.dedup_threshold,
            dedup_window=args.dedup_window, keep_audio=args.keep_audio, report=args.report,
            why=args.why, overwrite=args.overwrite, speakers=args.speakers,
            export=args.export,
            rtsp_transport=args.rtsp_transport,
            rtsp_max_runtime_seconds=args.rtsp_runtime,
            rtsp_chunk_seconds=args.rtsp_chunk,
            rtsp_read_timeout_seconds=args.rtsp_read_timeout,
            rtsp_frames_per_minute=args.rtsp_frames_per_minute,
            rtsp_max_retained_frames=args.rtsp_max_frames,
            rtsp_max_reconnects=args.rtsp_reconnects,
        )
    except Exception as e:  # noqa: BLE001 — surface a clean message to the user
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n✓ Done → {r.out_dir}")
    print(f"  {r.frame_count} frames  (deduped from {r.extracted_frames} extracted)  in {r.frames_dir}")
    print(f"  manifest:   {r.manifest_path}")
    if r.frames_json_path:
        print(f"  timestamps: {r.frames_json_path}  (per-frame source-video timestamps)")
    if args.viewer:
        from .viewer import write_viewer
        vsrc = os.path.join(r.out_dir, "source.mp4")
        vp = write_viewer(r.out_dir, vsrc if os.path.exists(vsrc) else (source if os.path.exists(source) else None))
        print(f"  viewer:     {vp}  (double-click to open)")
    if r.report_path:
        print(f"  report:     {r.report_path}  (open in a browser to tune the threshold)")
    if r.transcript_path:
        print(f"  transcript: {r.transcript_path}")
    else:
        print(f"  transcript: {r.transcript_note}")
    if r.audio_path:
        print(f"  audio:      {r.audio_path}  (full soundtrack — music + speech)")
    if args.grid:
        from .core import make_grids
        sheets = make_grids(r.frames_dir, r.out_dir)
        print(f"  grids:      {len(sheets)} contact sheet(s) in {r.out_dir}/grids")
    if args.kb:
        from .core import save_to_kb
        kb_source = "rtsp-stream-redacted" if source.lower().startswith("rtsp://") else source
        dest = save_to_kb(args.kb, r.manifest_path, kb_source)
        print(f"  knowledge base: {dest}")
    # one quiet pointer, opt out with CRV_NO_HINT=1
    if not os.environ.get("CRV_NO_HINT"):
        # use this run's real numbers, not ad copy — deduped keyframes are the
        # tool's own "the picture actually changed" count, so the claim is honest;
        # raw extraction counts would inflate static videos with fps-floor samples
        if r.duration and r.duration > 10 and r.frame_count >= 4:
            rate = (r.frame_count - 1) / r.duration * 60
            print(f"  pro:        this video has {r.frame_count - 1} real visual changes (~{rate:.0f}/min).")
            print("              Pro labels each camera move (pan/zoom/handheld), reads voice emotion,")
            print("              and builds a clickable timeline → https://leoaido.com/crv-pro/  ($19 founder price until Jul 31, then $29)")
        else:
            print("  pro:        camera-motion + voice-emotion analysis → https://leoaido.com/crv-pro/")


def _resolve_source(source: str | None, rtsp_source_file: str | None) -> str:
    if rtsp_source_file:
        if source:
            raise ValueError("use either source or --rtsp-source-file, not both")
        file_stat = os.stat(rtsp_source_file)
        if os.name == "posix" and stat.S_IMODE(file_stat.st_mode) & 0o077:
            raise ValueError("RTSP source file must be private (chmod 600)")
        with open(rtsp_source_file, encoding="utf-8") as source_file:
            value = source_file.read(8_193)
        if len(value) > 8_192:
            raise ValueError("RTSP source file is too large")
        value = value.strip()
        if not value.lower().startswith("rtsp://"):
            raise ValueError("RTSP source file must contain an rtsp:// URL")
        return value
    if not source:
        raise ValueError("source is required")
    if source.lower().startswith("rtsp://"):
        parts = urlsplit(source)
        if parts.username is not None or parts.password is not None:
            raise ValueError("authenticated RTSP URLs must use --rtsp-source-file")
    return source


if __name__ == "__main__":
    main()
