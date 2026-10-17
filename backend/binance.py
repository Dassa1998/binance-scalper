import hashlib, hmac, time
import httpx

URLS = {"testnet": ("https://testnet.binancefuture.com", "wss://fstream.binancefuture.com"),
        "mainnet": ("https://fapi.binance.com", "wss://fstream.binance.com")}


class Binance:
    """Minimal signed REST client for Binance USD-M Futures."""

    def __init__(self, key, secret, env):
        self.secret = secret.encode()
        self.rest, self.ws = URLS[env]
        self.http = httpx.AsyncClient(base_url=self.rest, timeout=10, headers={"X-MBX-APIKEY": key})

    async def call(self, method, path, signed=False, **p):
        if signed:
            p.update(timestamp=int(time.time() * 1000), recvWindow=5000)
            q = "&".join(f"{k}={v}" for k, v in p.items())
            q += "&signature=" + hmac.new(self.secret, q.encode(), hashlib.sha256).hexdigest()
            r = await self.http.request(method, f"{path}?{q}")
        else:
            r = await self.http.request(method, path, params=p)
        if r.status_code >= 400:
            raise RuntimeError(f"{path} {r.status_code}: {r.text[:200]}")
        return r.json()
