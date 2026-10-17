# Binance Futures Scalper

Stack: FastAPI + pandas (backend), single-page dashboard (frontend), SSE live updates.

## Deploy on Railway (phone friendly)
1. Push this repo to GitHub (keep .env out - Railway Variables hold the keys).
2. Railway > New Project > Deploy from GitHub repo. It builds the Dockerfile.
3. Settings > Region: choose **Singapore or Amsterdam** (Binance blocks US IPs: HTTP 451).
4. Variables: add everything in .env.example. DASHBOARD_TOKEN is REQUIRED (empty = dashboard locked).
5. Settings > Networking > Generate Domain, open it, enter your token.
6. Press **Connect** (validates keys), then **Auto-trading**. Start on **testnet**.

## Notes
- Account must be in One-way position mode (not Hedge).
- Auto-trading always starts OFF after a restart. Switching network turns it OFF.
- SL/TP use Binance's Algo Order API (/fapi/v1/algoOrder); if SL/TP placement fails the position is closed immediately.
- Fees: round trip is about 0.1% taker. Signals are skipped when TP distance < min_tp_pct (0.2%).
- No strategy guarantees profit. Test on testnet for days before mainnet, use money you can lose.
