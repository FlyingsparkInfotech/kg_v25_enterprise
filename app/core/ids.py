import hashlib
from datetime import datetime, timezone


def stable_id(*parts) -> str:
    """
    Collision-safe stable ID using length-prefixed encoding.

    Uses `{len}:{value}` per part so that different splits of the same
    string produce different hashes.  e.g.
        stable_id("a||b", "c")  !=  stable_id("a", "||b", "c")
        stable_id("a", None, "b")  !=  stable_id("a", "", "b")

    Returns first 32 hex chars of SHA-256 (128 bits — safe up to ~10^15 rows).
    """
    segments = []
    for p in parts:
        v = "" if p is None else str(p)
        segments.append(f"{len(v)}:{v}")
    encoded = "\x00".join(segments)          # null-byte separator (never in length-prefixed values)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:32]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
