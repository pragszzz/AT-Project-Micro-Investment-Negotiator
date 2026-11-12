# Run the real-time agent

## 1. Start it
```
pip install -r requirements.txt
python start_all.py
```
Your browser opens **http://localhost:8000**, the live dashboard. Press Ctrl+C in the
terminal to stop. (Python 3.10+.)

## 2. First check: are prices really live?
```
python market_api.py
```
It should print BTC/ETH/SOL prices and an AAPL quote. If it does, the dashboard
tiles show a green **LIVE** badge. If a feed is unreachable (offline, rate-limited),
that asset shows a gold **SIMULATED** badge and the Agent-mind panel says which feed
is down. The agent never silently mixes fake prices in.

## 3. What the agent does (all real code paths)
- **Perceive:** a background thread polls CoinGecko (BTC, ETH, SOL) and Yahoo Finance (AAPL, SPY).
- **Reason:** scores every asset x allocation size with the utility function, and
  checks open positions against take-profit and stop-loss.
- **Negotiate:** haggles the trading fee with the separate Broker process (signed messages).
- **Act:** in *Let it run* mode it trades on its own; in *Ask me first* mode each
  negotiated deal appears for you to Approve/Decline (expires after 120 s).
- **Account:** a $1000 **paper** account. Cash, holdings, P&L and trade history are
  saved to `paper_account.json`, so restarting keeps your account. "Reset paper
  account" starts over.

## 4. Settings you can change live (in the dashboard)
Risk tolerance, minimum trade, take-profit %, stop-loss %, and auto/ask mode.
Each change is logged in the "Belief revision log" and used from the next tick.

## 5. Notes
- US stocks (AAPL, SPY) only move during US market hours (about 7 PM to 1:30 AM IST).
  Outside those hours they show the last close, so crypto is where you'll see action.
- Paper trading only. No real money, no brokerage connection. The strategy is a simple
  "buy dips, take profits, cut losses" heuristic, not financial advice.
- Port already in use? Close the other program, or stop an earlier run.


```

