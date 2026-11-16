"""Deterministic randomness for playback.

The brief's rule: every random value a replay draws must come from a hash
of (seed, order_id, purpose), never from one shared generator drawn in
send order. A shared generator couples every value to how many orders
happened to be sent before it, so re-sending one order or changing the
in-flight bound silently changes every later draw; a hash keeps each
value a pure function of its own inputs.

Playback core itself draws no per-order random value yet (status-event
jitter, duplicate resends and delivery shuffling are later deliverables
and will call `stable_float`), but the rule is part of this PR's
contract with them.
"""

from __future__ import annotations

import hashlib

_MASK_64 = (1 << 64) - 1


def stable_digest(seed: int, order_id: str, purpose: str) -> bytes:
    """The 32-byte digest every random value is derived from."""
    return hashlib.sha256(f"{seed}:{purpose}:{order_id}".encode()).digest()


def stable_float(seed: int, order_id: str, purpose: str) -> float:
    """A uniform float in [0, 1) that depends on nothing but its inputs."""
    return int.from_bytes(stable_digest(seed, order_id, purpose)[:8], "big") / 2**64


def stable_int(seed: int, order_id: str, purpose: str, high: int) -> int:
    """A uniform int in [0, high) (high must be positive)."""
    if high <= 0:
        raise ValueError("high must be positive")
    return int.from_bytes(stable_digest(seed, order_id, purpose)[8:16], "big") % high
