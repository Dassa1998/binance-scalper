import asyncio, collections, json, math, os, time
import websockets
from binance import Binance
from strategy import signal

DEFAULTS = dict(env="testnet", tf="1m", leverage=5, margin="ISOLATED", risk_pct=1.0, sl_atr=1.5, tp_atr=1.2,
                rsi_lo=40, rsi_hi=60, max_pos=3, min_tp_pct=0.2, symbols="TOP50")


def rnd(x, step):
    p = max(0, -math.floor(math.log10(step)))
    return f"{math.floor(x / step + 1e-9) * step:.{p}f}"


class Engine:
    def __init__(self):
        self.cfg, self.auto = dict(DEFAULTS), False
        self.client, self.tasks, self.info, self.candles = None, [], {}, {}
        self.open, self.state = {}, {}
        self.day = dict(pnl=0.0, w=0, l=0)
        self.logs, self.subs, self.lock = collections.deque(maxlen=150), set(), asyncio.Lock()

    # ---- logging / SSE
    def log(self, m):
        line = f"{time.strftime('%H:%M:%S')} {m}"
        self.logs.append(line)
        self.emit({"t": "log", "m": line})

    def emit(self, ev):
        for q in list(self.subs):
            if q.qsize() < 200:
                q.put_nowait(ev)

    def snapshot(self):
        return {"t": "state", "connected": bool(self.tasks), "auto": self.auto, "cfg": self.cfg, **self.state, "day": self.day}

    # ---- lifecycle
    async def connect(self):
        await self.disconnect()
        env = self.cfg["env"]
        k, s = os.getenv(f"BINANCE_{env.upper()}_KEY"), os.getenv(f"BINANCE_{env.upper()}_SECRET")
        if not (k and s):
            return self.log(f"Set BINANCE_{env.upper()}_KEY and BINANCE_{env.upper()}_SECRET in environment variables")
        cl = Binance(k, s, env)
        try:
            await cl.call("GET", "/fapi/v2/account", True)  # validates keys
            ex = await cl.call("GET", "/fapi/v1/exchangeInfo")
        except Exception as e:
            await cl.http.aclose()
            return self.log(f"Connect failed: {e}")
        self.info = {}
        for x in ex["symbols"]:
            if x["quoteAsset"] == "USDT" and x["contractType"] == "PERPETUAL" and x["status"] == "TRADING":
                f = {y["filterType"]: y for y in x["filters"]}
                self.info[x["symbol"]] = dict(step=float(f["LOT_SIZE"]["stepSize"]), minq=float(f["LOT_SIZE"]["minQty"]),
                                              tick=float(f["PRICE_FILTER"]["tickSize"]),
                                              minn=float(f.get("MIN_NOTIONAL", {}).get("notional", 5)))
        self.client = cl
        self.log(f"API keys valid ({env})")
        self.tasks = [asyncio.create_task(self.run(fn)) for fn in (self.stream, self.poll)]
        self.emit(self.snapshot())

    async def disconnect(self):
        self.auto = False
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        if self.client:
            await self.client.http.aclose()
            self.client = None

    async def run(self, fn):  # supervisor: restart a worker if it crashes
        while True:
            try:
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log(f"{fn.__name__} error: {e}; restarting in 5s")
                await asyncio.sleep(5)

    # ---- market data
    async def pick(self):
        sel = self.cfg["symbols"]
        if sel != "TOP50":
            return [s for s in (x.strip().upper() for x in sel.split(",")) if s in self.info]
        t = [x for x in await self.client.call("GET", "/fapi/v1/ticker/24hr") if x["symbol"] in self.info]
        return [x["symbol"] for x in sorted(t, key=lambda x: -float(x["quoteVolume"]))[:50]]

    async def seed(self, syms):
        sem = asyncio.Semaphore(5)

        async def one(s):
            async with sem:
                k = await self.client.call("GET", "/fapi/v1/klines", symbol=s, interval=self.cfg["tf"], limit=250)
                self.candles[s] = collections.deque(([float(x[1]), float(x[2]), float(x[3]), float(x[4])] for x in k[:-1]), maxlen=300)
        await asyncio.gather(*(one(s) for s in syms))

    async def stream(self):
        syms = await self.pick()
        await self.seed(syms)
        url = f"{self.client.ws}/stream?streams=" + "/".join(f"{s.lower()}@kline_{self.cfg['tf']}" for s in syms)
        self.log(f"Scanning {len(syms)} pairs on {self.cfg['tf']}")
        delay = 1
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self.log("Market stream connected")
                    delay = 1
                    async for m in ws:
                        try:
                            await self.on_kline(json.loads(m)["data"]["k"])
                        except Exception as e:
                            self.log(f"Handler error: {e}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log(f"Stream dropped ({e}); reconnecting in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
                await self.seed(syms)  # refill candles missed while offline

    async def on_kline(self, k):
        if not k["x"]:
            return
        s, d = k["s"], self.candles.get(k["s"])
        if d is None:
            return
        d.append([float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"])])
        if not self.auto or s in self.open or len(self.open) >= self.cfg["max_pos"]:
            return
        sig = signal(list(d), self.cfg)
        if sig:
            await self.enter(s, *sig, float(k["c"]))

    # ---- execution
    async def enter(self, sym, side, sl, tp, px):
        c, cl, f = self.cfg, self.client, self.info[sym]
        async with self.lock:
            if sym in self.open or len(self.open) >= c["max_pos"]:
                return
            try:
                self.log(f"Signal {side} {sym} @ {px}")
                avail = self.state.get("avail", 0)
                cap = avail * c["leverage"] * 0.9 / px
                qty = min(avail * c["risk_pct"] / 100 / sl, cap)
                need = max(f["minn"] * 1.05 / px, f["minq"])  # exchange minimum notional
                if qty < need:
                    if need > cap:
                        return self.log(f"{sym}: balance/leverage too low for min order ({f['minn']} USDT)")
                    qty = need
                q = rnd(qty, f["step"])
                await cl.call("POST", "/fapi/v1/leverage", True, symbol=sym, leverage=c["leverage"])
                try:
                    await cl.call("POST", "/fapi/v1/marginType", True, symbol=sym, marginType=c["margin"])
                except RuntimeError as e:
                    if "-4046" not in str(e):  # -4046 = already that margin type
                        raise
                await cl.call("POST", "/fapi/v1/order", True, symbol=sym, side=side, type="MARKET", quantity=q)
            except Exception as e:
                return self.log(f"Order error {sym}: {e}")
            self.open[sym] = side
            try:  # position exists now: it MUST get SL/TP or be closed
                await asyncio.sleep(0.4)
                entry = float((await cl.call("GET", "/fapi/v2/positionRisk", True, symbol=sym))[0]["entryPrice"]) or px
                g, out = (1, "SELL") if side == "BUY" else (-1, "BUY")
                slp, tpp = rnd(entry - g * sl, f["tick"]), rnd(entry + g * tp, f["tick"])
                for typ, trig in (("STOP_MARKET", slp), ("TAKE_PROFIT_MARKET", tpp)):
                    await cl.call("POST", "/fapi/v1/algoOrder", True, algoType="CONDITIONAL", symbol=sym, side=out, type=typ,
                                  triggerPrice=trig, closePosition="true", workingType="MARK_PRICE")
                self.log(f"Filled {side} {sym} qty {q} entry {entry} | SL {slp} TP {tpp}")
            except Exception as e:
                self.log(f"SL/TP failed on {sym} ({e}); closing for safety")
                await self.close(sym)

    async def close(self, sym):
        cl = self.client
        amt = float((await cl.call("GET", "/fapi/v2/positionRisk", True, symbol=sym))[0]["positionAmt"])
        if amt:
            await cl.call("POST", "/fapi/v1/order", True, symbol=sym, side="SELL" if amt > 0 else "BUY", type="MARKET",
                          quantity=rnd(abs(amt), self.info[sym]["step"]), reduceOnly="true")
        try:
            await cl.call("DELETE", "/fapi/v1/algoOpenOrders", True, symbol=sym)
        except Exception:
            pass
        self.log(f"Close sent {sym}")

    # ---- account polling (balance, positions, daily PnL)
    async def poll(self):
        cl, n = self.client, 0
        while True:
            a = await cl.call("GET", "/fapi/v2/account", True)
            async with self.lock:  # don't race with enter()
                ps = [p for p in await cl.call("GET", "/fapi/v2/positionRisk", True) if float(p["positionAmt"])]
                now = {p["symbol"]: "BUY" if float(p["positionAmt"]) > 0 else "SELL" for p in ps}
                for s in set(self.open) - set(now):
                    self.log(f"Position closed {s}")
                    try:
                        await cl.call("DELETE", "/fapi/v1/algoOpenOrders", True, symbol=s)  # drop leftover SL/TP
                    except Exception:
                        pass
                self.open = now
            if n % 5 == 0:
                t0 = int(time.time() // 86400 * 86400 * 1000)
                inc = await cl.call("GET", "/fapi/v1/income", True, startTime=t0, limit=1000)
                rows = [i for i in inc if i["incomeType"] in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE")]
                r = [float(i["income"]) for i in rows if i["incomeType"] == "REALIZED_PNL"]
                self.day = dict(pnl=sum(float(i["income"]) for i in rows), w=sum(x > 0 for x in r), l=sum(x < 0 for x in r))
            n += 1
            self.state = dict(
                wallet=float(a["totalWalletBalance"]), margin=float(a["totalMarginBalance"]),
                avail=float(a["availableBalance"]), upnl=float(a["totalUnrealizedProfit"]),
                positions=[dict(s=p["symbol"], amt=float(p["positionAmt"]), entry=float(p["entryPrice"]), mark=float(p["markPrice"]),
                                liq=float(p["liquidationPrice"]), pnl=float(p["unRealizedProfit"]),
                                roe=float(p["unRealizedProfit"]) / max(abs(float(p["positionAmt"])) * float(p["entryPrice"]) / float(p["leverage"]), 1e-9) * 100)
                           for p in ps])
            self.emit(self.snapshot())
            await asyncio.sleep(2)
