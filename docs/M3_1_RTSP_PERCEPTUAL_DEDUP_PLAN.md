# M3.1 RTSP Perceptual Frame Selection Plan

> Status: design review candidate
>
> Scope: align RTSP token-saving frame selection with the existing batch
> pipeline without reopening G049 or changing its approved frame-only/security
> boundary.

## 1. Context and gap

The original batch pipeline saves model context by selecting visual evidence,
not by sending every decoded frame. Its decision path has two stages:

1. ffmpeg scene-change plus density-floor candidate extraction (or the
   two-pass adaptive variant);
2. `dedup_frames()` with a sliding window and three complementary channels:
   global RGB change, small-subject/action change, and settled-local change.

The current RTSP path is safe and bounded, but its semantic dedup is weaker.
It samples JPEG candidates at a fixed ceiling, hashes their encoded bytes with
BLAKE2, and uses `WindowDeduplicator` equality plus TTL. Compression noise,
clock overlays and small illumination changes can make visually equivalent
frames byte-different, so a static camera can still retain too many frames and
consume unnecessary agent image tokens.

This work is a new M3.1 increment. G049 remains the approved M3 secure-intake
baseline and keeps its current audit/release process. M3.1 must receive its own
architect review and changed-content release audit before shipping.

## 2. Definition of alignment

For an identical, source-time-ordered sequence of candidate images, batch and
RTSP must use the same perceptual decision engine, thresholds, sliding-window
semantics and selection reasons.

This first increment targets **selection/dedup parity**, which is the direct
token-saving behavior. It does not claim bit-for-bit candidate-extraction
parity with finite files:

- a live source has no complete-file second pass;
- RTSP M3 remains frame-only, so subtitle/text anchors are unavailable;
- the current RTSP candidate-rate ceiling remains the safety boundary during
  this increment.

Online scene-score candidate extraction may be added later, after the shared
selector is proven, without duplicating or replacing the perceptual engine.

## 3. Goals

- Reuse the batch global/action/settled-local detector in RTSP.
- Keep the default batch output and public contracts backward compatible.
- Suppress visually unchanged camera frames despite JPEG/noise differences.
- Preserve small local UI/text changes and small-subject motion.
- Keep source-time ordering, SSE replay, cancellation and reconnect semantics.
- Bound selector memory, candidate artifacts, CPU work and retained frames.
- Emit only selected artifacts to agents/viewers; raw candidate bytes remain
  local and short-lived.
- Quantify candidate-to-kept and estimated image-token reduction.

## 4. Non-goals

- RTSP audio, transcription or text anchors.
- Object recognition, semantic descriptions or cloud vision calls.
- Persisting camera identity, credentials or raw RTSP authority.
- Making `job_cleanup` a viewer-delivery guarantee.
- Replacing M1 lifecycle/SSE or M2 source clock/watermark ownership.
- Removing the existing runtime, read, reconnect, quota or frame ceilings.

## 5. Architecture decision

### 5.1 Extract one shared stateful selector

Move the decision logic currently nested in `core.dedup_frames()` into a
game/source-independent module, proposed as `frame_selection.py`.

```python
@dataclass(frozen=True)
class FrameCandidate:
    path: str
    media_time_ms: int

@dataclass(frozen=True)
class FrameDecision:
    candidate: FrameCandidate
    kept: bool
    reason: str                 # first/global/action/settled/perceptual_duplicate
    global_diff: float | None
    settled_score: float | None

class PerceptualFrameSelector:
    def __init__(self, *, threshold: float = 8, window: int = 4): ...
    def observe(self, candidate: FrameCandidate) -> list[FrameDecision]: ...
    def finish(self) -> list[FrameDecision]: ...
    def reconnect(self) -> list[FrameDecision]: ...
```

`observe()` returns the decision for the previous candidate because the
settled-local detector needs one-frame lookahead. `finish()` evaluates the
last candidate using the existing batch final-frame rule.

The shared selector retains exactly the current three channels and constants:

- 16x16 RGB global signature and percent-change threshold;
- 32x32 hard local/action cells;
- 192x192 settled-local signature, ±1-pixel tolerance, soft/hard gates and
  cooldown.

Do not substitute perceptual hashes. The mainline deliberately uses RGB change
because hashes can miss flat-color and equal-luma hue transitions.

### 5.2 Batch compatibility adapter

`dedup_frames()` remains the public batch helper. It becomes a filesystem
adapter around `PerceptualFrameSelector` and retains current behavior:

- chronological filenames and optional timestamps;
- dropped-file move/delete behavior;
- uniform `max_frames` thinning after perceptual selection;
- final rename format and optional report records.

Before RTSP integration, golden parity tests must prove that representative
batch fixtures produce the same keep/drop decisions and selection reasons as
the approved baseline.

### 5.3 RTSP placement

The selector sits after M2 has rejected late evidence and released candidates
in source-time order, but before exact retry dedup and public event emission:

```text
private RTSP URL
  -> bounded ffmpeg candidate spool
  -> source-time normalization
  -> M2 watermark release / late-evidence rejection
  -> PerceptualFrameSelector
  -> M2 exact retry dedup / decision adapter
  -> M1 JobEventBus/SSE/artifact endpoint
  -> agent reads selected frames + manifest only
```

This order is a hard invariant. A candidate rejected as late must never mutate
perceptual history, cooldown or pending lookahead state. This preserves the
G048 fix that moved exact dedup after watermark release.

The selector must not call the public event sink directly. M2 remains
responsible for source-time ordering, reconnect epochs, exact retry dedup and
the single event-emission path.

### 5.4 M2 decision adapter

Refactor the frame branch of `SegmentRunner` / `WindowEventProducer` into an
explicit ordered-candidate then decided-frame path rather than encoding
perceptual decisions as fake hashes:

```python
producer.frame_candidate(
    media_time_ms=candidate.media_time_ms,
    payload={"candidate_path": ..., "exact_signature": ...},
)
```

`frame_candidate()` first enters the existing bounded `SourceWatermark`.
Only candidates returned by watermark release are passed, in order, to
`PerceptualFrameSelector.observe()`. Decisions returned by `observe()` or
`finish()` then enter an internal decided-frame emission step; this step uses
`WindowDeduplicator` only for perceptually kept frames and emits through the
existing `WindowEventProducer` sink.

Rules:

- watermark release happens before any perceptual state mutation;
- late candidates are deleted without a selector decision or public frame
  event, matching the existing M2 late-evidence contract;
- perceptual drops emit the existing aggregated `frame_dropped` event with
  reason `perceptual_duplicate` and do not mutate exact-signature state;
- perceptual keeps may still pass through exact-signature TTL dedup as a cheap
  secondary guard against retry/replay duplicates;
- exact-signature drops delete their candidate artifact and use the existing
  `deduplicated` reason;
- no new public event type or schema version is required;
- existing event payload allowlists remain unchanged.

Direct `event_sink(FRAME_DROPPED, ...)` from `rtsp.py`, or invoking the
selector before watermark release, is rejected because either would bypass M2
ordering/state ownership and could recreate the G048 late-state-pollution bug.

## 6. Online state and reconnect contract

Selector state is hard bounded by configuration:

- one pending candidate for settled-local lookahead;
- one previous coarse signature;
- at most `dedup_window` kept 16x16, 32x32 and 192x192 signatures;
- one cooldown scalar and counters;
- `dedup_window` validation range: 1-32;
- `dedup_threshold` validation range: greater than 0 and at most 100.

At the default window of four, perceptual state is below 1 MiB. The selector
must expose a test-only state-size/count view so long-stream tests can assert
the bound without relying on implementation-private object sizes.

Reconnect is not a logical source replacement. For the same job/source, the
reset order is clock first, selector second, exact dedup third:

- `SourceWatermark.reset_epoch()` discards unreleased candidates; their spool
  files are deleted and they never reach the selector;
- discard the selector's one pending pre-disconnect candidate because it has
  no valid post-frame for settled-state evaluation;
- reset `previous_coarse` so motion is not measured across a network gap;
- preserve the recent kept perceptual window to avoid re-sending the same
  static camera view after every transient reconnect;
- reset the settled cooldown to its base value;
- continue using the existing monotonic M2 epoch clock;
- allow M2 exact-signature state to reset as it does today.

A new job or source URL creates a new selector with empty history. No visual
state is shared across jobs, users or source identities.

## 7. Candidate and artifact lifecycle

Current RTSP capture writes every sampled JPEG into the output frames tree.
M3.1 separates candidates from retained artifacts:

- ffmpeg writes to a per-epoch private candidate spool under the job output;
- each candidate is decoded once for selector signatures;
- a kept candidate is atomically moved to the public `frames/` tree;
- a dropped candidate is deleted immediately after its decision;
- the one-frame pending candidate is removed on cancellation/reconnect;
- cleanup remains owned by `JobManager` and existing quota enforcement;
- `max_retained_frames` counts kept/public artifacts, not discarded samples;
- candidate work remains bounded by runtime × `max_frames_per_minute`, chunk
  duration and the existing job disk quota.

The manifest records sampled, perceptually kept, exact-dedup dropped and
retained counts. It never records source authority or credentials.

## 8. Configuration contract

Reuse existing mainline options for RTSP instead of inventing conflicting
names:

- `dedup_threshold` / `--dedup-threshold` (default 8);
- `dedup_window` / `--dedup-window` (default 4).

They are already accepted by the CLI and `core.process()` but currently ignored
by `process_rtsp()`. M3.1 wires them through CLI -> core -> RTSP selector.

Web jobs add allowlisted numeric options with the same defaults and bounds.
The server must not accept arbitrary selector class names or executable filter
expressions from HTTP input.

`--adaptive`, `--scene` and `--text-anchors` remain batch-only in this first
increment. CLI/help/README must state this explicitly rather than silently
pretending full finite-file extraction parity.

## 9. Token-efficiency evidence

Add local-only counters:

- candidate frames;
- perceptually kept frames;
- perceptual drops;
- exact retry/replay drops;
- retained artifacts;
- reduction ratio;
- estimated image tokens before/after using the README formula
  `(width * height) / 750`.

Only aggregate counts and selection reasons may enter events/manifest. Image
signatures, camera metadata and raw frames do not enter the control stream.

## 10. Validation matrix

| Area | Required evidence |
|---|---|
| Batch parity | Golden fixtures produce identical keep/drop/reason records before and after refactor. |
| Static camera | At least 60 visually equivalent JPEG candidates with codec noise collapse to one retained frame. |
| Illumination/noise | Minor brightness and 1-pixel jitter do not create a frame flood. |
| Hard scene change | Three distinct scenes are all retained in source-time order. |
| A-B-A window | A repeated within the configured kept-frame window is dropped; it becomes eligible after enough distinct keeps. |
| Small action | Mainline small-subject fixture retains the action frames according to the existing action-channel baseline. |
| Settled local | Thin text/UI change is retained after settling; continuous motion/noise does not repeatedly fire. |
| One-frame lookahead | `observe()` delays the current decision; `finish()` handles the final candidate deterministically. |
| Late evidence | A candidate older than the emitted watermark is deleted before selector observation and cannot change a later decision. |
| Chunk boundary | A visual duplicate across RTSP chunks is dropped without resetting selector state. |
| Reconnect | Pending candidate is discarded, recent kept window survives, public timestamps remain monotonic, and static first frame is not re-sent. |
| Cancellation | Cancel during decode/selection/backoff removes pending candidate and leaves no orphan process/artifact. |
| Capacity | 10,000-candidate synthetic stream stays within selector/window/candidate-spool limits. |
| Events | `frame_kept` reasons are allowlisted; perceptual drops aggregate; lifecycle and SSE replay remain unchanged. |
| Security | Credentials/authority absent from argv, events, status, manifest, reports and retained metadata. |
| Real device | Bounded authenticated RTSP run records candidate/kept ratio and visual validity without committing address, credentials or camera frames. |
| CI | Ubuntu/macOS/Windows × Python 3.10-3.12, required ffmpeg fixture and full suite green. |

### Acceptance thresholds

- Static/noise fixture: at least 90% candidate reduction and exactly one kept
  visual state unless an intentional change is introduced.
- Three-scene fixture: 3/3 intended scene states retained.
- Small-action fixture: no regression from the batch action-channel baseline.
- Text/UI fixture: intended settled change retained, with no more than one keep
  for the same final state.
- No unbounded candidate files, selector signatures or event entries.
- Existing batch output, M1 lifecycle and approved M3 security tests do not
  regress.

## 11. Implementation sequence

### Commit 1: shared selector extraction

- Add `frame_selection.py` and unit tests.
- Reimplement `dedup_frames()` as an adapter.
- Lock batch golden parity before touching RTSP.

Validation: batch dedup tests, smoke suite, compileall, diff check.

### Commit 2: M2 ordered-candidate/decision contract

- Add source-time-ordered `frame_candidate()` composition so selector state is
  touched only after watermark release.
- Preserve exact TTL dedup as secondary retry protection.
- Add late-evidence isolation, ordering, capacity, drop aggregation and
  reconnect tests.

Validation: `test_stream_windows.py` plus M1 event/SSE lifecycle tests.

### Commit 3: RTSP perceptual integration

- Add candidate spool and selector lifecycle.
- Wire threshold/window through core, CLI and web.
- Delete dropped candidates and count retained artifacts correctly.
- Add static/noise/action/chunk/reconnect/cancel fixtures.

Validation: RTSP tests, real ffmpeg fixture and bounded authenticated manual run.

### Commit 4: agent-facing evidence and docs

- Update manifest counters, README usage and token-reduction evidence.
- Keep source/credential redaction probes.
- Run full cross-platform CI and changed-content audit package.

## 12. Rollback and compatibility

- Batch public flags and output filenames remain stable.
- RTSP default thresholds match batch defaults.
- A temporary internal fallback may retain exact-hash-only RTSP selection for
  rollback, but it is not exposed as a public long-term mode.
- If the shared selector causes a batch parity regression, stop before Commit 2
  and keep G049 unchanged.
- M3.1 changes only the newly modified selector/RTSP/M2 adapter surface; fork
  original code outside the diff remains outside changed-content audit scope.

## 13. Architect review questions

1. Is the proposed M2 composition boundary correct: watermark releases an
   ordered candidate, the shared selector decides it, then exact retry dedup
   and public emission run in the same `WindowEventProducer` path?
2. On transient reconnect, should recent perceptual kept history be preserved
   as proposed, or fully reset to match current exact-signature behavior?
3. Is selection/dedup parity an acceptable M3.1 boundary before online
   scene-score candidate extraction, or must fixed/adaptive scene selection be
   included in the same milestone?
4. Should optional perceptual scores remain local/report-only, or may they be
   added to `frames.json` without a schema version change?
5. Are the candidate-spool and selector state bounds sufficient for the M3
   security/resource gate?

## 14. Definition of done

M3.1 is complete only when:

- architect approves this boundary and the implementation diff;
- one shared perceptual selector drives both batch and RTSP decisions;
- batch golden parity and all validation-matrix cases pass;
- static-camera token reduction meets the threshold without missing scene,
  local-text or small-action fixtures;
- real authenticated RTSP evidence is collected without persisting source or
  credentials;
- full 3 OS × 3 Python CI is green;
- changed-content release audit and final release authorization are complete.
