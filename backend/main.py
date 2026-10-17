import asyncio, json, os, secrets
from pathlib import Path
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from engine import DEFAULTS, Engine

app, eng = FastAPI(), Engine()
TOKEN = os.getenv("DASHBOARD_TOKEN", "")
CHOICES = dict(env=("testnet", "mainnet"), tf=("1m", "3m", "5m"), margin=("ISOLATED", "CROSSED"))


def auth(x_token: str = Header(""), token: str = Query("")):
    if not TOKEN or not secrets.compare_digest(x_token or token, TOKEN):  # no token set = locked
        raise HTTPException(401)


A = [Depends(auth)]


@app.post("/api/connect", dependencies=A)
async def connect():
    await eng.connect()
    eng.emit(eng.snapshot())
    return {"ok": bool(eng.client)}


@app.post("/api/config", dependencies=A)
async def config(body: dict):
    old = dict(eng.cfg)
    for k, v in body.items():
        if k not in DEFAULTS:
            continue
        if k in CHOICES and v not in CHOICES[k]:
            raise HTTPException(400, f"bad {k}")
        eng.cfg[k] = type(DEFAULTS[k])(v)
    eng.cfg["leverage"] = max(1, min(50, eng.cfg["leverage"]))
    eng.cfg["risk_pct"] = max(0.1, min(10.0, eng.cfg["risk_pct"]))
    if eng.client and any(old[k] != eng.cfg[k] for k in ("env", "tf", "symbols")):
        await eng.connect()  # also switches auto-trading off
    eng.emit(eng.snapshot())
    return {"ok": True}


@app.post("/api/auto", dependencies=A)
async def auto(body: dict):
    eng.auto = bool(body.get("on")) and bool(eng.client)
    eng.log(f"Auto-trading {'ON' if eng.auto else 'OFF'}")
    eng.emit(eng.snapshot())
    return {"auto": eng.auto}


@app.post("/api/close/{sym}", dependencies=A)
async def close(sym: str):
    if not eng.client:
        raise HTTPException(400, "not connected")
    await eng.close(sym.upper())
    return {"ok": True}


@app.post("/api/closeall", dependencies=A)
async def closeall():
    eng.auto = False
    for s in list(eng.open):
        await eng.close(s)
    eng.emit(eng.snapshot())
    return {"ok": True}


@app.get("/api/stream", dependencies=A)
async def stream(request: Request):
    q = asyncio.Queue()
    eng.subs.add(q)

    async def gen():
        try:
            for l in list(eng.logs):
                yield f"data: {json.dumps({'t': 'log', 'm': l})}\n\n"
            yield f"data: {json.dumps(eng.snapshot())}\n\n"
            while True:
                try:
                    ev = await asyncio.wait_for(q.get(), 15)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
                    continue
                yield f"data: {json.dumps(ev)}\n\n"
        finally:
            eng.subs.discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


app.mount("/", StaticFiles(directory=Path(__file__).resolve().parent.parent / "frontend", html=True))
