"""crv web — local batch viewer with replayable realtime job events.

Paste a video URL (YouTube, Instagram, ...) or a file path, run the analysis,
then open the result viewer. Stdlib only, runs 100% locally. UI ships in
Traditional Chinese, Simplified Chinese and English (toggle, persisted)."""
import http.server
import json
import mimetypes
import os
import shutil
import subprocess
import sys
import threading
import time
from urllib.parse import parse_qs, urlparse
import webbrowser

from .core import ProcessingCancelled, make_grids, process
from .job_events import JOB_DONE, JOB_ERROR, JOB_LOG, JobEvent, JobEventBus
from .viewer import write_viewer

JOBS: dict = {}  # id -> {state, log, out_dir, err, bus}

PAGE = """<!doctype html>
<html lang="zh-Hant"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>crv Web</title>
<style>
  :root { color-scheme: dark }
  * { box-sizing:border-box; margin:0; padding:0 }
  body { background:#0d0b07; color:#e8dfcf; font-family:Menlo,Consolas,monospace; min-height:100vh;
         display:flex; flex-direction:column; align-items:center; padding:48px 20px }
  .lang { position:fixed; top:16px; right:20px; display:flex; gap:6px }
  .lang button { font:inherit; font-size:12px; color:#8a7d63; background:#14110b;
         border:1px solid #2a2418; border-radius:8px; padding:4px 10px; cursor:pointer; margin:0; font-weight:normal }
  .lang button.on { color:#e8b64c; border-color:#e8b64c }
  h1 { color:#e8b64c; font-size:22px; letter-spacing:1px }
  .sub { color:#8a7d63; font-size:13px; margin-top:8px; text-align:center; max-width:720px; line-height:1.7 }
  form { width:100%; max-width:720px; margin-top:36px }
  input[type=text] { width:100%; font:inherit; font-size:15px; color:#e8dfcf; background:#14110b;
         border:1px solid #3a3323; border-radius:12px; padding:14px 16px; outline:none }
  input[type=text]:focus { border-color:#e8b64c }
  .opts { display:flex; flex-wrap:wrap; gap:10px 22px; margin-top:16px; font-size:13.5px; color:#c9bda3 }
  .opts label { cursor:pointer; display:flex; align-items:center; gap:7px }
  .opts small { color:#6d6350 }
  #go { font:inherit; font-size:14.5px; margin-top:22px; cursor:pointer; color:#0d0b07;
         background:#e8b64c; border:none; border-radius:10px; padding:12px 28px; font-weight:bold }
  #go:disabled { background:#4a4130; color:#8a7d63; cursor:default }
  #log { width:100%; max-width:720px; margin-top:26px; font-size:12.5px; line-height:1.7; color:#8a7d63;
         white-space:pre-wrap; border:1px solid #2a2418; border-radius:12px; padding:14px 16px;
         background:#14110b; min-height:80px; max-height:40vh; overflow:auto; display:none }
  #done { display:none; margin-top:18px }
  #done button { font:inherit; font-size:14.5px; cursor:pointer; color:#0d0b07; background:#7cc36a;
         border:none; border-radius:10px; padding:12px 28px; font-weight:bold }
  #cancel { display:none; font:inherit; font-size:13px; cursor:pointer; color:#d88373; background:transparent;
         border:1px solid #744338; border-radius:8px; padding:8px 14px; margin:14px 0 0 }
  #frames { width:100%; max-width:720px; display:none; margin-top:22px; grid-template-columns:repeat(auto-fill,minmax(140px,1fr)); gap:8px }
  #frames a { display:block; border:1px solid #2a2418; border-radius:8px; overflow:hidden; background:#14110b; color:#c9bda3; font-size:10px; text-decoration:none }
  #frames img { display:block; width:100%; aspect-ratio:16/9; object-fit:cover; background:#0d0b07 }
  #frames span { display:block; padding:4px 6px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap }
</style></head><body>
<div class="lang">
  <button data-lang="zh_tw">繁中</button><button data-lang="zh_cn">简中</button><button data-lang="en">EN</button>
</div>
<h1 data-i="title"></h1>
<div class="sub" data-i="sub"></div>
<form id="f">
  <input type="text" id="src" data-p="ph" autofocus>
  <div class="opts">
    <label><input type="checkbox" id="adaptive"> <span data-i="adaptive"></span> <small data-i="adaptive_h"></small></label>
    <label><input type="checkbox" id="text_anchors"> <span data-i="ta"></span> <small data-i="ta_h"></small></label>
    <label><input type="checkbox" id="grid" checked> <span data-i="grid"></span> <small data-i="grid_h"></small></label>
    <label><input type="checkbox" id="transcribe" checked> <span data-i="tr"></span></label>
  </div>
  <button type="submit" id="go"></button>
</form>
<div id="log"></div>
<div id="frames"></div>
<button id="cancel" type="button" data-i="cancel"></button>
<div id="done"><button id="openv"></button></div>
<p style="margin-top:28px;font-size:12px;opacity:.55;text-align:center">Built in public by <a href="https://x.com/LeoAidoAI" target="_blank" style="color:inherit">@LeoAidoAI</a></p>
<script>
const I18N = {
  zh_tw: { title:"crv 網頁版",
    sub:"貼上 YouTube / Instagram Reels 連結或本機影片路徑，AI 就能真的看懂這支影片。全程在你的電腦上跑，不上傳任何東西。",
    ph:"https://www.youtube.com/watch?v=...  或  /Users/you/video.mp4",
    adaptive:"慢變化內容", adaptive_h:"（教學、手寫、慢速運鏡）",
    ta:"字幕感知", ta_h:"（字卡、簡報、螢幕錄影）",
    grid:"九宮格", grid_h:"（省 token）", tr:"逐字稿",
    go:"開始分析", running:"分析中...", starting:"啟動中...", cancel:"取消分析", cancelled:"已取消",
    open:"開啟結果頁", failed:"失敗" },
  zh_cn: { title:"crv 网页版",
    sub:"粘贴 YouTube / Instagram Reels 链接或本机视频路径，AI 就能真的看懂这条视频。全程在你的电脑上跑，不上传任何东西。",
    ph:"https://www.youtube.com/watch?v=...  或  /Users/you/video.mp4",
    adaptive:"慢变化内容", adaptive_h:"（教学、手写、慢速运镜）",
    ta:"字幕感知", ta_h:"（字卡、演示文稿、屏幕录制）",
    grid:"九宫格", grid_h:"（省 token）", tr:"逐字稿",
    go:"开始分析", running:"分析中...", starting:"启动中...", cancel:"取消分析", cancelled:"已取消",
    open:"打开结果页", failed:"失败" },
  en: { title:"crv Web",
    sub:"Paste a YouTube / Instagram Reels link or a local file path — your AI gets to actually watch the video. Runs 100% on your machine, nothing is uploaded.",
    ph:"https://www.youtube.com/watch?v=...  or  /Users/you/video.mp4",
    adaptive:"Slow-changing", adaptive_h:"(tutorials, handwriting, slow pans)",
    ta:"Text anchors", ta_h:"(captions, slides, screen recordings)",
    grid:"Contact sheets", grid_h:"(saves tokens)", tr:"Transcript",
    go:"Analyze", running:"Running...", starting:"Starting...", cancel:"Cancel analysis", cancelled:"Cancelled",
    open:"Open results", failed:"Failed" }
};
let L = localStorage.getItem('crv_lang') || 'zh_tw';
let T = I18N[L];
function applyLang(l){
  L=l; T=I18N[l]; localStorage.setItem('crv_lang', l);
  document.documentElement.lang = l==='en'?'en':(l==='zh_cn'?'zh-Hans':'zh-Hant');
  document.querySelectorAll('[data-i]').forEach(el=>el.textContent=T[el.dataset.i]);
  document.querySelectorAll('[data-p]').forEach(el=>el.placeholder=T[el.dataset.p]);
  document.querySelectorAll('.lang button').forEach(b=>b.classList.toggle('on', b.dataset.lang===l));
  const go=document.getElementById('go');
  go.textContent = go.disabled ? T.running : T.go;
  document.getElementById('openv').textContent = T.open;
}
document.querySelectorAll('.lang button').forEach(b=>b.addEventListener('click',()=>applyLang(b.dataset.lang)));
applyLang(L);

const f=document.getElementById('f'), log=document.getElementById('log'),
      done=document.getElementById('done'), go=document.getElementById('go'),
      frames=document.getElementById('frames'), cancel=document.getElementById('cancel');
let jid=null, stream=null;
function endRun(message, isDone){
  if(stream){ stream.close(); stream=null; }
  go.disabled=false; go.textContent=T.go; cancel.style.display='none';
  if(isDone) done.style.display='block';
  if(message){ log.textContent += '\\n' + message; log.scrollTop=log.scrollHeight; }
}
function addFrame(data){
  const path=data.artifact;
  if(!path) return;
  const a=document.createElement('a'), img=document.createElement('img'), label=document.createElement('span');
  const url='/artifacts?id='+encodeURIComponent(jid)+'&path='+encodeURIComponent(path);
  a.href=url; a.target='_blank'; img.src=url; img.alt=data.frame||path;
  label.textContent=(data.frame||path)+(data.timestamp_seconds==null?'':' · '+data.timestamp_seconds.toFixed(2)+'s');
  a.append(img,label); frames.append(a); frames.style.display='grid';
}
function connectEvents(){
  stream=new EventSource('/events?id='+encodeURIComponent(jid));
  stream.addEventListener('job_log', e=>{ const d=JSON.parse(e.data).data; if(d.message){ log.textContent += '\\n'+d.message; log.scrollTop=log.scrollHeight; } });
  stream.addEventListener('frame_kept', e=>addFrame(JSON.parse(e.data).data));
  stream.addEventListener('job_done', ()=>endRun('', true));
  stream.addEventListener('job_cancelled', ()=>endRun(T.cancelled, false));
  stream.addEventListener('job_error', e=>{ const d=JSON.parse(e.data).data; endRun(T.failed+': '+(d.message||''), false); });
}
f.addEventListener('submit', async e=>{
  e.preventDefault();
  const src=document.getElementById('src').value.trim();
  if(!src) return;
  go.disabled=true; go.textContent=T.running; done.style.display='none'; cancel.style.display='inline-block';
  log.style.display='block'; log.textContent=T.starting;
  frames.replaceChildren(); frames.style.display='none';
  const opts={adaptive:adaptive.checked, text_anchors:text_anchors.checked,
              grid:grid.checked, transcribe:transcribe.checked};
  const r=await fetch('/run',{method:'POST',headers:{'Content-Type':'application/json'},
              body:JSON.stringify({src, opts})});
  const payload=await r.json();
  if(!r.ok){ endRun(T.failed+': '+(payload.error||''), false); return; }
  jid=payload.id; connectEvents();
});
cancel.addEventListener('click', async ()=>{ if(jid) await fetch('/cancel?id='+encodeURIComponent(jid),{method:'POST'}); });
document.getElementById('openv').addEventListener('click', ()=>fetch('/open?id='+jid));
</script></body></html>"""


def _run_job(jid: str, src: str, opts: dict) -> None:
    job = JOBS[jid]
    out = job["out_dir"]
    bus: JobEventBus = job["bus"]

    def event_sink(event_type: str, data: dict) -> None:
        # core finishes its batch artifacts before the web-only viewer/grid
        # artifacts. Hold job_done until those web artifacts are ready too.
        if event_type == JOB_DONE:
            job["done_event"] = dict(data)
            return
        bus.emit(jid, event_type, data)
        if event_type == JOB_LOG:
            job["log"] += f"{data.get('message', '')}\n"

    try:
        result = process(
            src, out,
            adaptive=bool(opts.get("adaptive")),
            text_anchors=bool(opts.get("text_anchors")),
            do_transcribe=bool(opts.get("transcribe", True)),
            event_sink=event_sink,
            cancel_check=job["cancel_event"].is_set,
        )
        if job["cancel_event"].is_set():
            job["state"] = "cancelled"
            return
        if opts.get("grid"):
            make_grids(result.frames_dir, out)
        write_viewer(out, result.video)
        job["state"] = "done"
        done_event = job.pop("done_event", None)
        if done_event is None:
            done_event = {"frame_count": result.frame_count}
        bus.emit(jid, JOB_DONE, done_event)
    except ProcessingCancelled:
        job["state"] = "cancelled"
    except Exception as e:  # noqa: BLE001 — whatever failed, show it in the UI
        job["state"], job["err"] = "error", str(e)
        if not bus.is_terminal(jid):
            bus.emit(jid, JOB_ERROR, {"error_type": type(e).__name__, "message": str(e)})
    finally:
        if bus.is_terminal(jid) and not bus.has_cleanup(jid):
            bus.cleanup(jid, "worker finished")


def _query_value(path: str, key: str) -> str | None:
    return parse_qs(urlparse(path).query).get(key, [None])[0]


def _safe_artifact_path(out_dir: str, artifact: str) -> str | None:
    """Return an existing file only when it resolves inside this job's output."""
    root = os.path.realpath(out_dir)
    candidate = os.path.realpath(os.path.join(root, artifact))
    if os.path.commonpath((root, candidate)) != root:
        return None
    return candidate if os.path.isfile(candidate) else None


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):  # silence request logging
        pass

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _events(self, jid: str, since: int) -> None:
        job = JOBS.get(jid)
        if job is None:
            return self._json({"error": "unknown job"}, 404)
        bus: JobEventBus = job["bus"]
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        try:
            while True:
                events = bus.wait_for_events(jid, since=since, timeout=15.0)
                if events:
                    for event in events:
                        self._write_sse_event(event)
                        since = event.seq
                    if bus.is_terminal(jid):
                        return
                elif bus.is_terminal(jid):
                    return
                else:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

    def _write_sse_event(self, event: JobEvent) -> None:
        payload = json.dumps(event.to_dict(), ensure_ascii=False, separators=(",", ":"))
        message = f"id: {event.seq}\nevent: {event.type}\ndata: {payload}\n\n"
        self.wfile.write(message.encode("utf-8"))
        self.wfile.flush()

    def _artifact(self, jid: str, artifact: str | None) -> None:
        job = JOBS.get(jid)
        if job is None:
            return self._json({"error": "unknown job"}, 404)
        if not artifact:
            return self._json({"error": "missing artifact path"}, 400)
        path = _safe_artifact_path(job["out_dir"], artifact)
        if path is None:
            return self._json({"error": "artifact not found"}, 404)
        content_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(os.path.getsize(path)))
        self.end_headers()
        with open(path, "rb") as f:
            shutil.copyfileobj(f, self.wfile)

    def do_GET(self):
        if self.path == "/":
            body = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path.startswith("/status"):
            jid = _query_value(self.path, "id")
            j = JOBS.get(jid)
            self._json({"state": j["state"], "log": j["log"][-4000:], "err": j.get("err")}
                       if j else {"state": "error", "err": "unknown job"})
        elif self.path.startswith("/events"):
            jid = _query_value(self.path, "id")
            raw_since = _query_value(self.path, "since") or self.headers.get("Last-Event-ID") or "0"
            try:
                since = max(0, int(raw_since))
            except ValueError:
                return self._json({"error": "invalid event sequence"}, 400)
            self._events(jid or "", since)
        elif self.path.startswith("/artifacts"):
            self._artifact(_query_value(self.path, "id") or "", _query_value(self.path, "path"))
        elif self.path.startswith("/open"):
            jid = _query_value(self.path, "id")
            j = JOBS.get(jid)
            if j:
                viewer = os.path.join(j["out_dir"], "viewer.html")
                opener = "open" if sys.platform == "darwin" else "xdg-open"
                if shutil.which(opener) and os.path.exists(viewer):
                    subprocess.Popen([opener, viewer])
                else:
                    webbrowser.open("file://" + viewer)
            self._json({"ok": True})
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path.startswith("/cancel"):
            jid = _query_value(self.path, "id") or ""
            job = JOBS.get(jid)
            if job is None:
                return self._json({"error": "unknown job"}, 404)
            if not job["bus"].is_terminal(jid):
                job["cancel_event"].set()
                job["bus"].cancel(jid, "user requested")
                job["state"] = "cancelling"
            return self._json({"state": job["state"]})
        if self.path != "/run":
            return self.send_error(404)
        n = int(self.headers.get("Content-Length", 0))
        data = json.loads(self.rfile.read(n) or b"{}")
        src = (data.get("src") or "").strip()
        if not src:
            return self._json({"error": "missing src"}, 400)
        jid = str(int(time.time() * 1000))
        out = os.path.join(os.path.expanduser("~/crv-web-out"), jid)
        JOBS[jid] = {
            "state": "running", "log": "", "out_dir": out,
            "bus": JobEventBus(), "cancel_event": threading.Event(),
        }
        threading.Thread(target=_run_job, args=(jid, src, data.get("opts") or {}),
                         daemon=True).start()
        self._json({"id": jid})


def main() -> None:
    port = 8642
    if len(sys.argv) > 2 and sys.argv[1] == "--port":
        port = int(sys.argv[2])
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    url = f"http://127.0.0.1:{port}/"
    print(f"crv Web: {url}  (Ctrl+C to stop)")
    threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
