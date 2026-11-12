"""
agent_server.py - the integrated, real-time Micro-Investment Negotiator Agent

Everything from the course in one running system:
  Unit 1  perceive -> reason -> act loop, condition-action exits (take-profit / stop-loss)
  Unit 2  background threads (feed worker + agent loop) guarded by a lock;
          signed messages to a separate Broker process over HTTP
  Unit 3  utility-scored option search + multi-round fee negotiation
  Unit 4  BDI structures (beliefs / desires / intentions + belief log),
          knowledge base, live REST market APIs
  Unit 5  HMAC-signed broker traffic, API-key protected commands
  UI      serves dashboard.html at http://localhost:8000

TRADING IS PAPER TRADING ONLY: a simulated account is marked to market against
real (or, if feeds are unreachable, clearly-labelled simulated) prices.
The strategy is a simple mean-reversion heuristic, not financial advice.

Run:  python start_all.py     (or: python broker_agent.py  +  python agent_server.py)
"""

import json
import os
import random
import threading
import time
from datetime import datetime

import requests
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from market_api import MarketAPIError, fetch_coingecko_batch, fetch_yahoo_quote
from security import sign, verify

# ---------------------------------------------------------------------------
# Configuration (all overridable with environment variables)
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BROKER_URL = os.getenv("BROKER_URL", "http://localhost:8001")
DASHBOARD_API_KEY = "demo-dashboard-key-2026"   # demo only; real systems use per-client secrets
ACCOUNT_FILE = os.path.join(BASE_DIR, "paper_account.json")

FEED_INTERVAL = float(os.getenv("FEED_INTERVAL", 15))     # seconds between market polls
AGENT_INTERVAL = float(os.getenv("AGENT_INTERVAL", 4))    # seconds between agent decisions
SIM_VOL_MULT = float(os.getenv("SIM_VOL_MULT", 1))        # exaggerates fallback volatility (testing)
STARTING_CASH = float(os.getenv("STARTING_CASH", 1000))

COOLDOWN_SECONDS = float(os.getenv("COOLDOWN_SECONDS", 90))   # no re-trading an asset right away
PENDING_TTL = float(os.getenv("PENDING_TTL", 120))            # ask-mode proposals expire
MAX_TRADE_PCT = 0.20        # one trade <= 20% of total portfolio value
MAX_POSITION_PCT = 0.40     # one asset <= 40% of total portfolio value
ALLOCATION_LEVELS = 4
ESTIMATED_FEE_PCT = 0.4     # used for scoring only; the real fee comes from negotiation
MAX_NEGOTIATION_ROUNDS = 4
BUYER_WALK_AWAY_FEE_PCT = 0.45

with open(os.path.join(BASE_DIR, "knowledge_base.json")) as f:
    KNOWLEDGE_BASE = json.load(f)["assets"]
ASSETS = list(KNOWLEDGE_BASE)

# ---------------------------------------------------------------------------
# Shared state. RLock because helpers (push_ledger etc.) are called both with
# and without the lock already held. Every read/write of `state` is under it.
# ---------------------------------------------------------------------------
state_lock = threading.RLock()
feed_ready = threading.Event()
cooldown_until = {}
_next_id = 1

state = {
    "mode": "ask", "risk_tolerance": 3.0, "min_trade": 10.0,
    "take_profit_pct": 5.0, "stop_loss_pct": 6.0,
    "cash": STARTING_CASH, "starting_value": STARTING_CASH,
    "positions": {}, "realized_pnl": 0.0, "trades": [],
    "prices": {a: {"price": None, "change_pct": 0.0, "source": "pending", "updated": None} for a in ASSETS},
    "feed": {"coingecko": {"ok": None, "error": None, "last_ok": None},
             "yahoo": {"ok": None, "error": None, "last_ok": None}},
    "history": [], "intention": "Waiting for the first market data...", "intention_kind": "WAITING",
    "options_evaluated": 0, "pending": None, "broker_online": None,
    "desires": ["Grow portfolio value over time",
                "Never exceed the user's configured risk tolerance",
                "Minimize fees paid through negotiation"],
    "belief_log": [], "ledger": [], "started_at": time.time(),
}
PERSISTED = ("cash", "positions", "realized_pnl", "trades", "starting_value",
             "mode", "risk_tolerance", "min_trade", "take_profit_pct", "stop_loss_pct")


def now_str():
    return datetime.now().strftime("%H:%M:%S")


def push_ledger(text, amount=None):
    global _next_id
    with state_lock:
        state["ledger"] = [{"id": _next_id, "t": now_str(), "text": text, "amount": amount}] + state["ledger"][:59]
        _next_id += 1


def push_belief(note):
    with state_lock:
        state["belief_log"] = [{"t": now_str(), "note": note}] + state["belief_log"][:39]


def total_value():
    with state_lock:
        v = state["cash"]
        for a, pos in state["positions"].items():
            p = state["prices"][a]["price"]
            if p:
                v += pos["qty"] * p
        return v


def set_intention(kind, text):
    with state_lock:
        if kind != state["intention_kind"]:
            push_belief(f"Intention shifted: {state['intention_kind']} -> {kind}")
            state["intention_kind"] = kind
        state["intention"] = text


def save_account():
    with state_lock:
        data = {k: state[k] for k in PERSISTED}
    tmp = ACCOUNT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, ACCOUNT_FILE)


def load_account():
    if not os.path.exists(ACCOUNT_FILE):
        return False
    try:
        with open(ACCOUNT_FILE) as f:
            data = json.load(f)
        with state_lock:
            for k in PERSISTED:
                if k in data:
                    state[k] = data[k]
        return True
    except (OSError, ValueError):
        return False


# ---------------------------------------------------------------------------
# PERCEIVE: market feed worker (background daemon thread, Unit 2)
# ---------------------------------------------------------------------------
_sim = {}


def set_price(asset, price, change, source):
    with state_lock:
        prev = state["prices"][asset]["source"]
        state["prices"][asset] = {"price": round(price, 6 if price < 1 else 2), "change_pct": round(change, 2),
                                  "source": source, "updated": time.time()}
        if prev != source:
            push_belief(f"{asset} price source: {prev} -> {source}")


def simulate(asset):
    """Clearly-labelled fallback: a mean-reverting random walk anchored on the
    last known real price, so the account never sees an artificial price jump."""
    kb = KNOWLEDGE_BASE[asset]
    with state_lock:
        last = state["prices"][asset]["price"]
    s = _sim.get(asset)
    if s is None:
        base = last or kb["sim_start_price"]
        s = _sim[asset] = {"ref": base, "price": base}
    vol = kb["sim_vol"] * SIM_VOL_MULT
    s["price"] *= 1 + random.gauss(0, vol) + 0.04 * (s["ref"] / s["price"] - 1)
    set_price(asset, s["price"], (s["price"] / s["ref"] - 1) * 100, "simulated")


def _feed_status(provider, ok, error=None):
    with state_lock:
        f = state["feed"][provider]
        if ok != f["ok"] and f["ok"] is not None:
            push_belief(f"{provider} feed {'recovered' if ok else 'went down'}")
        f["ok"], f["error"] = ok, error
        if ok:
            f["last_ok"] = time.time()


def poll_crypto():
    ids = {a: kb["api_id"] for a, kb in KNOWLEDGE_BASE.items() if kb["provider"] == "coingecko"}
    try:
        data = fetch_coingecko_batch(list(ids.values()))
        for a, cid in ids.items():
            price, change = data[cid]
            _sim.pop(a, None)
            set_price(a, price, change, "live")
        _feed_status("coingecko", True)
    except MarketAPIError as e:
        _feed_status("coingecko", False, str(e)[:160])
        for a in ids:
            simulate(a)


def poll_yahoo():
    ok_any, last_err = False, None
    for a, kb in KNOWLEDGE_BASE.items():
        if kb["provider"] != "yahoo":
            continue
        try:
            price, change = fetch_yahoo_quote(kb["api_id"])
            _sim.pop(a, None)
            set_price(a, price, change, "live")
            ok_any = True
        except MarketAPIError as e:
            last_err = str(e)[:160]
            simulate(a)
    _feed_status("yahoo", ok_any, None if ok_any else last_err)


def feed_worker():
    cycle = 0
    while True:
        poll_crypto()
        if cycle % 2 == 0:          # stocks refresh at half the crypto rate
            poll_yahoo()
        cycle += 1
        feed_ready.set()
        time.sleep(FEED_INTERVAL)


# ---------------------------------------------------------------------------
# REASON: utility-scored option search (Unit 3.2) with knowledge-base volatility
# ---------------------------------------------------------------------------
def compute_utility(asset, change_pct, alloc, risk_tolerance):
    expected_return = -change_pct / 100                      # dips tend to mean-revert (heuristic)
    fee = ESTIMATED_FEE_PCT / 100
    excess = max(0.0, abs(change_pct) - risk_tolerance)
    penalty = alloc * 0.015 * excess * KNOWLEDGE_BASE[asset]["volatility_multiplier"]
    return alloc * expected_return - alloc * fee - penalty


def generate_options(blocked):
    """Every (asset x allocation level) the account can afford. Caller holds the lock."""
    options = []
    total = total_value()
    cap = min(state["cash"], MAX_TRADE_PCT * total)
    min_trade = state["min_trade"]
    for asset, p in state["prices"].items():
        if p["price"] is None or asset in blocked:
            continue
        pos = state["positions"].get(asset)
        held = pos["qty"] * p["price"] if pos else 0.0
        asset_cap = min(cap, MAX_POSITION_PCT * total - held)
        if asset_cap < min_trade:
            continue
        for level in range(1, ALLOCATION_LEVELS + 1):
            alloc = round(min_trade + (asset_cap - min_trade) * level / ALLOCATION_LEVELS, 2)
            u = compute_utility(asset, p["change_pct"], alloc, state["risk_tolerance"])
            options.append({"asset": asset, "price": p["price"], "change": p["change_pct"],
                            "alloc": alloc, "utility": round(u, 4)})
    return options


def decide_locked():
    """One reasoning step. Returns a decision dict or None. Caller holds the lock."""
    now = time.time()
    blocked = {a for a, t in cooldown_until.items() if now < t}

    # 1) Condition-action exits come first: protect gains and cut losses.
    for asset, pos in state["positions"].items():
        p = state["prices"][asset]["price"]
        if not p or asset in blocked:
            continue
        pnl_pct = (pos["qty"] * p / pos["cost"] - 1) * 100
        reason = None
        if pnl_pct >= state["take_profit_pct"]:
            reason = f"take-profit: position up {pnl_pct:.1f}% (target {state['take_profit_pct']}%)"
        elif pnl_pct <= -state["stop_loss_pct"]:
            reason = f"stop-loss: position down {pnl_pct:.1f}% (limit -{state['stop_loss_pct']}%)"
        if reason:
            set_intention("NEGOTIATE_SELL", f"Selling {asset}: {reason}")
            return {"action": "SELL", "asset": asset, "price": p, "qty": pos["qty"], "reason": reason,
                    "change": state["prices"][asset]["change_pct"], "alloc": round(pos["qty"] * p, 2),
                    "utility": None, "options_evaluated": 0}

    # 2) Otherwise score every affordable option and take the best.
    options = generate_options(blocked)
    state["options_evaluated"] = len(options)
    if not options:
        set_intention("OBSERVE", "Observing: no affordable option right now (cash, caps or cooldowns)")
        return None
    best = max(options, key=lambda o: o["utility"])
    if best["utility"] <= 0:
        set_intention("OBSERVE", f"Observing: evaluated {len(options)} options, best is {best['asset']} "
                                 f"(utility {best['utility']:.2f}), not worth acting on")
        return None
    set_intention("NEGOTIATE_BUY", f"Negotiating BUY {best['asset']} ${best['alloc']:.2f}: best of "
                                   f"{len(options)} options (utility {best['utility']:.2f}, 24h change {best['change']}%)")
    return {"action": "BUY", "asset": best["asset"], "price": best["price"], "change": best["change"],
            "alloc": best["alloc"], "utility": best["utility"], "options_evaluated": len(options),
            "reason": f"{best['asset']} moved {best['change']}% in 24h, within tolerance, highest utility"}


# ---------------------------------------------------------------------------
# ACT: negotiate with the Broker agent (signed, Units 3.1 + 5.1), then trade
# ---------------------------------------------------------------------------
def negotiate_with_broker(asset, price):
    rounds = []
    try:
        qp = {"asset": asset, "price": price}
        q = requests.post(f"{BROKER_URL}/quote", json=qp, headers={"X-Signature": sign(qp)}, timeout=3).json()
        if not verify({"asset": q["asset"], "fee_pct": q["fee_pct"]}, q.get("signature")):
            return False, None, [{"note": "broker quote failed signature check, aborting"}]
        ask = q["fee_pct"]
    except (requests.exceptions.RequestException, KeyError, ValueError):
        with state_lock:
            state["broker_online"] = False
        return False, None, [{"note": "Broker agent unreachable (is broker_agent.py running?)"}]

    with state_lock:
        state["broker_online"] = True
    offer = round(ask * 0.55, 2)
    for n in range(1, MAX_NEGOTIATION_ROUNDS + 1):
        try:
            body = {"asset": asset, "price": price, "buyer_offer_pct": offer, "broker_last_ask_pct": ask}
            r = requests.post(f"{BROKER_URL}/negotiate", json=body, headers={"X-Signature": sign(body)}, timeout=3).json()
            if not verify({"status": r["status"], "fee_pct": r["fee_pct"]}, r.get("signature")):
                return False, None, rounds + [{"note": f"round {n}: broker reply failed signature check"}]
        except (requests.exceptions.RequestException, KeyError, ValueError):
            return False, None, rounds + [{"note": "Broker agent unreachable mid-negotiation"}]
        rounds.append({"round": n, "buyer_offer_pct": offer, "broker_response": r["status"], "broker_fee_pct": r["fee_pct"]})
        if r["status"] == "accept":
            return True, r["fee_pct"], rounds
        if r["status"] == "reject":
            return False, None, rounds
        ask = r["fee_pct"]
        if ask <= BUYER_WALK_AWAY_FEE_PCT:
            return True, ask, rounds
        offer = round((offer + ask) / 2, 2)
    return False, None, rounds


def execute_trade(deal, how):
    """Apply a trade to the paper account at the CURRENT live price. Caller holds the lock."""
    asset = deal["asset"]
    price = state["prices"][asset]["price"]
    fee_pct = deal["fee_pct"]
    if not price:
        push_ledger(f"CANCELLED {deal['action']} {asset} - no price available")
        return
    if deal["action"] == "BUY":
        alloc = min(deal["alloc"], state["cash"])
        if alloc < state["min_trade"]:
            push_ledger(f"CANCELLED BUY {asset} - not enough cash (${state['cash']:.2f})")
            return
        fee = alloc * fee_pct / 100
        qty = (alloc - fee) / price
        pos = state["positions"].setdefault(asset, {"qty": 0.0, "cost": 0.0})
        pos["qty"] += qty
        pos["cost"] += alloc
        state["cash"] -= alloc
        trade = {"action": "BUY", "asset": asset, "qty": qty, "price": price, "value": alloc, "fee_pct": fee_pct, "pnl": None}
        push_ledger(f"BUY {asset} · {qty:.6g} @ ${price:,.2f} · fee {fee_pct}% · {how}", alloc)
    else:
        pos = state["positions"].get(asset)
        if not pos:
            push_ledger(f"CANCELLED SELL {asset} - no position")
            return
        gross = pos["qty"] * price
        net = gross * (1 - fee_pct / 100)
        pnl = net - pos["cost"]
        state["cash"] += net
        state["realized_pnl"] += pnl
        del state["positions"][asset]
        trade = {"action": "SELL", "asset": asset, "qty": pos["qty"], "price": price, "value": net, "fee_pct": fee_pct, "pnl": pnl}
        push_ledger(f"SELL {asset} · {pos['qty']:.6g} @ ${price:,.2f} · fee {fee_pct}% · P&L {pnl:+.2f} · {how}", net)
    trade.update({"t": now_str(), "reason": deal.get("reason")})
    state["trades"] = [trade] + state["trades"][:199]
    cooldown_until[asset] = time.time() + COOLDOWN_SECONDS
    save_account()


def agent_loop():
    feed_ready.wait(timeout=60)
    while True:
        time.sleep(AGENT_INTERVAL)
        decision = None
        with state_lock:
            p = state["pending"]
            if p and time.time() - p["created"] > PENDING_TTL:
                push_ledger(f"EXPIRED {p['action']} {p['asset']} - no answer within {int(PENDING_TTL)}s")
                cooldown_until[p["asset"]] = time.time() + 30
                state["pending"] = None
            state["history"] = (state["history"] + [{"t": time.time(), "v": round(total_value(), 2)}])[-400:]
            if state["pending"] is None:
                decision = decide_locked()
        if decision is None:
            continue

        accepted, fee_pct, rounds = negotiate_with_broker(decision["asset"], decision["price"])  # slow I/O, no lock held

        with state_lock:
            if state["pending"] is not None:
                continue
            if not accepted:
                note = rounds[-1].get("note", "no agreement reached") if rounds else "no agreement reached"
                push_ledger(f"NO DEAL {decision['action']} {decision['asset']} · {note}")
                cooldown_until[decision["asset"]] = time.time() + 20
                set_intention("OBSERVE", "Observing: negotiation failed, trying other options")
                continue
            deal = {**decision, "fee_pct": fee_pct, "rounds": rounds, "created": time.time()}
            if state["mode"] == "ask":
                state["pending"] = deal
                set_intention("AWAIT_USER", f"Waiting for your approval: {deal['action']} {deal['asset']} at {fee_pct}% fee")
            else:
                execute_trade(deal, "auto")
                set_intention("OBSERVE", "Observing the market")


# ---------------------------------------------------------------------------
# Web layer (dashboard + API). Reads are open; commands need the API key.
# ---------------------------------------------------------------------------
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def require_auth(x_api_key: str = Header(default=None)):
    if x_api_key != DASHBOARD_API_KEY:
        raise HTTPException(status_code=401, detail="missing or invalid API key - request rejected")
    return True


class Settings(BaseModel):
    mode: str | None = None
    risk_tolerance: float | None = Field(default=None, ge=0.1, le=20)
    min_trade: float | None = Field(default=None, ge=1, le=10000)
    take_profit_pct: float | None = Field(default=None, ge=0.5, le=100)
    stop_loss_pct: float | None = Field(default=None, ge=0.5, le=100)


@app.get("/")
def dashboard():
    return FileResponse(os.path.join(BASE_DIR, "dashboard.html"))


@app.get("/state")
def get_state():
    with state_lock:
        total = total_value()
        positions, invested = {}, 0.0
        for a, pos in state["positions"].items():
            price = state["prices"][a]["price"] or 0.0
            val = pos["qty"] * price
            invested += val
            positions[a] = {"qty": pos["qty"], "avg_cost": pos["cost"] / pos["qty"], "price": price, "value": val,
                            "unrealized": val - pos["cost"], "unrealized_pct": (val / pos["cost"] - 1) * 100}
        out = {k: state[k] for k in ("mode", "risk_tolerance", "min_trade", "take_profit_pct", "stop_loss_pct",
                                      "cash", "realized_pnl", "starting_value", "prices", "feed", "history",
                                      "intention", "intention_kind", "options_evaluated", "broker_online",
                                      "desires", "belief_log", "ledger", "trades")}
        out["trades"] = out["trades"][:30]
        out.update(positions=positions, invested=invested, portfolio=total,
                   pnl=total - state["starting_value"], pnl_pct=(total / state["starting_value"] - 1) * 100,
                   uptime=time.time() - state["started_at"], now=time.time(),
                   pending=None if not state["pending"] else
                   {**state["pending"], "expires_in": max(0, PENDING_TTL - (time.time() - state["pending"]["created"]))})
        return json.loads(json.dumps(out))


@app.post("/settings")
def update_settings(s: Settings, _a: bool = Depends(require_auth)):
    if s.mode is not None and s.mode not in ("auto", "ask"):
        raise HTTPException(status_code=422, detail="mode must be 'auto' or 'ask'")
    with state_lock:
        for key in ("mode", "risk_tolerance", "min_trade", "take_profit_pct", "stop_loss_pct"):
            new = getattr(s, key)
            if new is not None and new != state[key]:
                push_belief(f"User changed {key}: {state[key]} -> {new} (next tick replans against this)")
                state[key] = new
        # Switching to auto while a proposal is waiting: the user has just told
        # the agent to act on its own, so it carries out the deal it already
        # negotiated instead of leaving the whole loop blocked on a stale prompt.
        if s.mode == "auto" and state["pending"]:
            execute_trade(state["pending"], "auto, mode switched while pending")
            state["pending"] = None
            set_intention("OBSERVE", "Observing the market")
        save_account()
    return {"ok": True}


@app.post("/approve")
def approve(_a: bool = Depends(require_auth)):
    with state_lock:
        deal = state["pending"]
        if deal:
            execute_trade(deal, "you approved")
            state["pending"] = None
            set_intention("OBSERVE", "Observing the market")
    return {"ok": True}


@app.post("/decline")
def decline(_a: bool = Depends(require_auth)):
    with state_lock:
        deal = state["pending"]
        if deal:
            push_ledger(f"DECLINED {deal['action']} {deal['asset']} · you said no")
            cooldown_until[deal["asset"]] = time.time() + COOLDOWN_SECONDS
            state["pending"] = None
            set_intention("OBSERVE", "Observing the market")
    return {"ok": True}


@app.post("/reset")
def reset_account(_a: bool = Depends(require_auth)):
    with state_lock:
        state.update(cash=STARTING_CASH, starting_value=STARTING_CASH, positions={}, realized_pnl=0.0,
                     trades=[], pending=None, history=[])
        cooldown_until.clear()
        push_belief("Paper account reset by user")
        push_ledger("Paper account reset")
        save_account()
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn

    restored = load_account()
    push_belief("Agent started" + (" (paper account restored from disk)" if restored else " with a fresh $%.0f paper account" % STARTING_CASH))
    push_ledger("Agent initialized")
    threading.Thread(target=feed_worker, daemon=True, name="feed-worker").start()
    threading.Thread(target=agent_loop, daemon=True, name="agent-loop").start()
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
