"""
security.py

Assignment 5.1: Agent Authentication and Host Integrity Verification.

Shared by both agent processes (Buyer and Broker) so every message between
them can be signed and verified. Uses HMAC-SHA256 with a shared secret —
this is the same family of technique real systems use for API request
signing (AWS Signature V4, webhook signature verification, etc.), just
simplified for a course project.

PRODUCTION NOTE (be upfront about this, don't pretend otherwise)
------------------------------------------------------------------
A hardcoded shared secret is fine for demonstrating the *mechanism*, but a
real deployment would: generate a unique key per agent, store it in a secret
manager (not source code), rotate it periodically, and likely use asymmetric
signing (public/private keypairs) so the Broker never has to hold a secret
capable of impersonating the Buyer, and vice versa. The verification logic
below would be identical either way — only key management changes.
"""

import hashlib
import hmac
import json

SHARED_SECRET = b"micro-invest-demo-shared-secret-2026"


def canonical_bytes(payload: dict) -> bytes:
    """Deterministic serialization so signer and verifier always hash the
    exact same bytes, regardless of dict key insertion order."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def sign(payload: dict) -> str:
    return hmac.new(SHARED_SECRET, canonical_bytes(payload), hashlib.sha256).hexdigest()


def verify(payload: dict, signature: str) -> bool:
    """Constant-time comparison — a naive `==` on signatures is itself a
    security bug (timing attacks), so this uses hmac.compare_digest."""
    if not signature:
        return False
    expected = sign(payload)
    return hmac.compare_digest(expected, signature)
