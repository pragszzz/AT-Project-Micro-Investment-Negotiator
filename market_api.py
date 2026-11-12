"""
market_api.py

Real-time market data for the agent (Assignment 4.2: external REST APIs).

Two key-less public sources:
  * CoinGecko  - crypto (BTC, ETH, SOL). ONE batched call returns every coin,
                 which keeps us well inside the free rate limit.
  * Yahoo Finance chart endpoint - stocks/ETFs (AAPL, SPY). Unofficial but
                 widely used and needs no key. Prices only move while the US
                 market is open; outside those hours the last close is returned.

Every function raises MarketAPIError on ANY failure (no network, rate limit,
bad JSON, missing field) so the caller can fall back gracefully instead of
crashing the agent.
"""

import requests

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
HEADERS = {"User-Agent": "Mozilla/5.0 (MicroInvestAgent/1.0)", "Accept": "application/json"}


class MarketAPIError(Exception):
    pass


def fetch_coingecko_batch(coin_ids, vs_currency="usd", timeout=6.0):
    """Returns {coin_id: (price, change_pct_24h)} for all requested coins."""
    try:
        resp = requests.get(
            COINGECKO_URL,
            params={"ids": ",".join(coin_ids), "vs_currencies": vs_currency, "include_24hr_change": "true"},
            headers=HEADERS, timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        out = {}
        for cid in coin_ids:
            entry = data[cid]
            price = float(entry[vs_currency])
            change = round(float(entry.get(f"{vs_currency}_24h_change") or 0.0), 2)
            if price <= 0:
                raise ValueError(f"non-positive price for {cid}")
            out[cid] = (price, change)
        return out
    except Exception as e:
        raise MarketAPIError(f"CoinGecko fetch failed: {e}") from e


def fetch_yahoo_quote(symbol, timeout=6.0):
    """Returns (price, change_pct_vs_previous_close) for a stock/ETF symbol."""
    try:
        resp = requests.get(
            YAHOO_URL.format(symbol=symbol),
            params={"range": "1d", "interval": "1d"},
            headers=HEADERS, timeout=timeout,
        )
        resp.raise_for_status()
        meta = resp.json()["chart"]["result"][0]["meta"]
        price = float(meta["regularMarketPrice"])
        prev = float(meta.get("chartPreviousClose") or meta.get("previousClose"))
        if price <= 0 or prev <= 0:
            raise ValueError("non-positive price")
        return price, round((price / prev - 1) * 100, 2)
    except Exception as e:
        raise MarketAPIError(f"Yahoo fetch failed for {symbol}: {e}") from e


if __name__ == "__main__":
    # Run `python market_api.py` on YOUR machine to confirm live data works.
    try:
        print("CoinGecko:", fetch_coingecko_batch(["bitcoin", "ethereum", "solana"]))
    except MarketAPIError as e:
        print(e)
    try:
        print("Yahoo AAPL:", fetch_yahoo_quote("AAPL"))
    except MarketAPIError as e:
        print(e)
