"""
broker_agent.py

This is a SEPARATE, independent agent process — the Broker. It has its own
goals (maximize its fee) that conflict with the Buyer Agent's goals (minimize
the fee it pays). That conflict is what makes this a genuine negotiation
rather than a fake back-and-forth — each side is a self-interested agent
with its own utility function, per Unit 3's brief.

Run this on its own, separate from agent_server.py:
    python broker_agent.py
It listens on http://localhost:8001

PROTOCOL (a simplified contract-net style negotiation)
--------------------------------------------------------
1. Buyer asks for a quote on an asset  -> POST /quote
2. Broker proposes an opening fee percentage
3. Buyer counters with a lower fee     -> POST /negotiate
4. Broker either:
      - ACCEPTs the buyer's offer (if it's above its floor), or
      - COUNTERs with something between its last ask and the buyer's offer, or
      - REJECTs if the buyer won't move and the broker has hit its floor
5. Buyer decides to accept the best offer received or walk away
   (this decision logic lives in agent_server.py)
"""

import random

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from security import sign, verify

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# The broker's own private utility parameters — the buyer never sees these
# directly, only the offers that result from them. This mirrors real-world
# information asymmetry between a trader and a market maker.
BASE_FEE_PCT = 0.60      # the broker's opening ask (typical exchange taker-fee range)
FLOOR_FEE_PCT = 0.15     # the broker will not go below this, no matter what


class QuoteRequest(BaseModel):
    asset: str
    price: float


class NegotiateRequest(BaseModel):
    asset: str
    price: float
    buyer_offer_pct: float
    broker_last_ask_pct: float


@app.post("/quote")
def quote(req: QuoteRequest, x_signature: str = Header(default=None)):
    payload = {"asset": req.asset, "price": req.price}
    if not verify(payload, x_signature):
        raise HTTPException(status_code=401, detail="invalid or missing signature — request rejected")

    opening = round(BASE_FEE_PCT + random.uniform(-0.05, 0.10), 2)
    response = {"asset": req.asset, "fee_pct": opening}
    return {**response, "signature": sign(response)}


@app.post("/negotiate")
def negotiate(req: NegotiateRequest, x_signature: str = Header(default=None)):
    payload = {
        "asset": req.asset, "price": req.price,
        "buyer_offer_pct": req.buyer_offer_pct, "broker_last_ask_pct": req.broker_last_ask_pct,
    }
    if not verify(payload, x_signature):
        raise HTTPException(status_code=401, detail="invalid or missing signature — request rejected")

    buyer_offer = req.buyer_offer_pct
    last_ask = req.broker_last_ask_pct

    if buyer_offer >= last_ask:
        response = {"status": "accept", "fee_pct": last_ask}
    elif buyer_offer >= FLOOR_FEE_PCT + 0.15:
        new_ask = max(round((last_ask + buyer_offer) / 2, 2), FLOOR_FEE_PCT)
        response = {"status": "counter", "fee_pct": new_ask}
    elif last_ask <= FLOOR_FEE_PCT + 0.05:
        response = {"status": "reject", "fee_pct": last_ask}
    else:
        new_ask = max(FLOOR_FEE_PCT, round(last_ask - 0.1, 2))
        response = {"status": "counter", "fee_pct": new_ask}

    return {**response, "signature": sign(response)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="warning")
