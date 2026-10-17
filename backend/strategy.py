import pandas as pd


def signal(rows, cfg):
    """rows: [[open, high, low, close], ...] closed candles. Returns (side, sl_dist, tp_dist) or None.
    Trend filter (EMA 9/21/200) AND trigger (RSI pullback-turn OR Bollinger band re-entry)."""
    if len(rows) < 210:
        return None
    df = pd.DataFrame(rows, columns=["o", "h", "l", "c"])
    c = df.c
    e9, e21, e200 = (c.ewm(span=n, adjust=False).mean() for n in (9, 21, 200))
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi = 100 - 100 / (1 + up / dn.replace(0, 1e-12))
    mid, sd = c.rolling(20).mean(), c.rolling(20).std(ddof=0)
    bu, bl = mid + 2 * sd, mid - 2 * sd
    tr = pd.concat([df.h - df.l, (df.h - c.shift()).abs(), (df.l - c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / 14, adjust=False).mean().iloc[-1]
    px = c.iloc[-1]
    long_up = px > e200.iloc[-1] and e9.iloc[-1] > e21.iloc[-1]
    short_dn = px < e200.iloc[-1] and e9.iloc[-1] < e21.iloc[-1]
    long_t = (rsi.iloc[-2] < cfg["rsi_lo"] and rsi.iloc[-1] > rsi.iloc[-2]) or (df.l.iloc[-2] <= bl.iloc[-2] and px > bl.iloc[-1])
    short_t = (rsi.iloc[-2] > cfg["rsi_hi"] and rsi.iloc[-1] < rsi.iloc[-2]) or (df.h.iloc[-2] >= bu.iloc[-2] and px < bu.iloc[-1])
    side = "BUY" if long_up and long_t else "SELL" if short_dn and short_t else None
    if not side or not atr > 0:
        return None
    sl, tp = atr * cfg["sl_atr"], atr * cfg["tp_atr"]
    if tp / px * 100 < cfg["min_tp_pct"]:  # TP must clear round-trip taker fees (~0.1%)
        return None
    return side, sl, tp
