"""
Public mode: the guards LIME applies when it is open to the internet.

Turn it on with the environment variable ``GLHE_PUBLIC=1`` (``deploy/run.sh``
does this with ``GLHE_PUBLIC=1 deploy/run.sh``). Then:

* a run is stopped after ``GLHE_MAX_RUN_MINUTES`` (default 45);
* lakes larger than ``GLHE_MAX_LAKE_AREA_KM2`` (default 10,000 km²), or whose
  size is unknown, cannot be run (prebuild those yourself in private mode);
* a lake that was already processed opens from its results; it is not rerun;
* each visitor may start one run every ``GLHE_COOLDOWN_MINUTES`` (default 10);
* only the visitor who started a run can cancel it;
* the Panel ``/admin`` page is off (see deploy/Containerfile).

CLAY still runs one lake at a time for the whole server, as in private mode.
"""

from __future__ import annotations

import os
import time

import panel as pn


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def enabled() -> bool:
    """True when the app is running in public mode."""
    return os.environ.get("GLHE_PUBLIC", "").strip().lower() in ("1", "true", "yes")


def max_run_seconds() -> float | None:
    """Time limit per CLAY run, or None (no limit) outside public mode."""
    return _env_float("GLHE_MAX_RUN_MINUTES", 45) * 60 if enabled() else None


def max_lake_area_km2() -> float:
    return _env_float("GLHE_MAX_LAKE_AREA_KM2", 10_000)


def cooldown_seconds() -> float:
    return _env_float("GLHE_COOLDOWN_MINUTES", 10) * 60


_COOLDOWN_CACHE_KEY = "glhe_public_last_run"


def visitor_id() -> str:
    """
    Who is asking, as well as we can tell: the client IP that Cloudflare
    (``CF-Connecting-IP``) or Tailscale Funnel/Serve (``X-Forwarded-For``)
    forwards, else this browser session's ID.
    """
    headers = {k.lower(): v for k, v in (pn.state.headers or {}).items()}
    if headers.get("cf-connecting-ip"):
        return headers["cf-connecting-ip"].strip()
    forwarded = headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    ctx = getattr(pn.state.curdoc, "session_context", None)
    return getattr(ctx, "id", None) or "unknown"


def _last_runs() -> dict:
    return pn.state.cache.setdefault(_COOLDOWN_CACHE_KEY, {})


def record_run(visitor: str, now: float | None = None) -> None:
    """Remember that ``visitor`` started a run (for the cooldown)."""
    _last_runs()[visitor] = time.time() if now is None else now


def lake_block_reason(lake: dict | None, processed_ids: set[int]) -> str | None:
    """
    Why this lake cannot be run in public mode (short button text), or None.

    Not visitor-specific, so it can be shown on the Run button.
    """
    if not lake or lake.get("hylak_id") is None:
        return None
    if lake["hylak_id"] in processed_ids:
        return "Already processed"
    area = lake.get("area_km2")
    if area is None:
        return "Lake size unknown"
    if area > max_lake_area_km2():
        return "Too large for public runs"
    return None


def visitor_block_reason(visitor: str, now: float | None = None) -> str | None:
    """A message if ``visitor`` is still in their cooldown, else None."""
    last = _last_runs().get(visitor)
    if last is None:
        return None
    wait = cooldown_seconds() - ((time.time() if now is None else now) - last)
    if wait <= 0:
        return None
    return f"You started a run recently; try again in {max(1, round(wait / 60))} min."


BANNER = (
    "Research prototype: GLHE has not been scientifically validated. Check "
    "results against the caveats and the original data before using them."
)
