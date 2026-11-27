"""The fault layer (WP6b): break link groups on purpose, heal them exactly.

A fault acts on a link group from `common.cluster.link_groups` - never on
one proxy by name - and gets one fault id, so `heal` undoes exactly what
that fault did (architecture Sections 6.3 and 7.3):

| Fault     | Toxiproxy action                               |
| --------- | ---------------------------------------------- |
| `cut`     | disable every proxy in the group               |
| `silence` | `timeout` toxic, `timeout: 0`, both directions |
| `latency` | `latency` toxic, both directions               |
| `reset`   | `reset_peer` toxic, both directions            |

The groups overlap (`r1-standby__r1-primary` is in `node:r1-primary` and
in `repl:r1`), which gives two rules:

- **Cut counts per proxy.** A proxy is disabled when its first cut
  arrives and re-enabled only when its last cut is healed; healing one
  cut never re-opens a link another cut still holds.
- **Toxic names come from the fault id** (`f3-latency-up`): Toxiproxy's
  default name is `<type>_<stream>`, so two latency faults on
  overlapping groups would otherwise collide on a shared proxy, and a
  heal would remove the other fault's toxic.

Fault ids live in this object only. After the control process restarts
the ids are gone while the toxics and disabled proxies stay in
Toxiproxy: `links()` lists those as `unexplained`, and `heal_all()`
(Toxiproxy's own reset) clears them.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from restaurantos.sim.toxiproxy import Toxiproxy, ToxiproxyError

FAULT_KINDS = ("cut", "silence", "latency", "reset")

# fault kind -> Toxiproxy toxic type (cut has none: it disables proxies)
TOXIC_TYPES = {"silence": "timeout", "latency": "latency", "reset": "reset_peer"}

# (suffix in the toxic name, Toxiproxy stream)
_DIRECTIONS = (("up", "upstream"), ("down", "downstream"))

# the params each kind accepts, with their defaults (None: required)
_PARAMS: dict[str, dict[str, int | None]] = {
    "cut": {},
    "silence": {},
    "latency": {"latency_ms": None, "jitter_ms": 0},
    "reset": {"timeout_ms": 0},
}


class FaultError(Exception):
    """A fault request the layer refuses. `status` and `code` are the
    control API's answer (architecture Section 6.3)."""

    status = 400
    code = "fault_error"


class UnknownGroupError(FaultError):
    status = 404
    code = "unknown_group"


class UnknownFaultError(FaultError):
    status = 404
    code = "unknown_fault"


class FaultExistsError(FaultError):
    status = 409
    code = "fault_exists"

    def __init__(self, message: str, fault_id: str) -> None:
        super().__init__(message)
        self.fault_id = fault_id


class FaultParamsError(FaultError, ValueError):
    status = 422
    code = "invalid_params"


@dataclass(frozen=True)
class Fault:
    fault_id: str
    target: str
    fault: str
    params: dict[str, int]
    proxies: tuple[str, ...]
    created_at: str
    # (proxy, toxic name) for every toxic this fault added; empty for a cut
    toxics: tuple[tuple[str, str], ...] = ()
    # proxies this cut found already disabled: not ours to re-enable
    preexisting: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        """The fault as Section 6.3 answers it."""
        return {
            "fault_id": self.fault_id,
            "target": self.target,
            "fault": self.fault,
            "params": dict(self.params),
            "proxies": list(self.proxies),
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class Healed:
    """A heal's answer: the fault it undid and the proxies it left
    disabled because they were already disabled before any cut."""

    fault: Fault
    left_disabled: tuple[str, ...]


def normalize_params(fault: str, params: Mapping[str, Any] | None) -> dict[str, int]:
    """Check a fault's params against Section 6.3 and fill the defaults.

    Every value is a whole number of milliseconds, 0 or more; an unknown
    key is refused rather than ignored, so a typo never runs a fault
    with a default nobody asked for."""
    if fault not in _PARAMS:
        raise FaultParamsError(f"unknown fault {fault!r}; one of {', '.join(FAULT_KINDS)}")
    allowed = _PARAMS[fault]
    given = dict(params or {})
    unknown = sorted(set(given) - set(allowed))
    if unknown:
        accepted = ", ".join(allowed) or "none"
        raise FaultParamsError(
            f"{fault} does not take {', '.join(unknown)} (params it takes: {accepted})"
        )
    out: dict[str, int] = {}
    for key, default in allowed.items():
        value = given.get(key, default)
        if value is None:
            raise FaultParamsError(f"{fault} needs {key}")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise FaultParamsError(f"{key} must be a whole number of milliseconds, 0 or more")
        out[key] = value
    return out


def toxic_attributes(fault: str, params: Mapping[str, int]) -> dict[str, int]:
    """The Toxiproxy attributes of a toxic fault."""
    if fault == "silence":
        return {"timeout": 0}
    if fault == "latency":
        return {"latency": params["latency_ms"], "jitter": params["jitter_ms"]}
    if fault == "reset":
        return {"timeout": params["timeout_ms"]}
    raise ValueError(f"{fault!r} adds no toxic")


class FaultLayer:
    """Faults on link groups, with one id each. One per control process."""

    def __init__(self, toxiproxy: Toxiproxy, groups: Mapping[str, Sequence[str]]) -> None:
        self._toxiproxy = toxiproxy
        self._groups = {name: tuple(members) for name, members in groups.items()}
        self._faults: dict[str, Fault] = {}
        self._cut_counts: dict[str, int] = {}
        # proxies that were already disabled when their first cut arrived:
        # a heal must leave them disabled (the state is not ours)
        self._pre_disabled: set[str] = set()
        self._ids = itertools.count(1)
        # one change at a time: a create and a heal must not interleave
        # their Toxiproxy calls on a shared proxy
        self._lock = asyncio.Lock()

    @property
    def groups(self) -> dict[str, tuple[str, ...]]:
        return dict(self._groups)

    def faults(self) -> list[Fault]:
        return list(self._faults.values())

    def cut_count(self, proxy: str) -> int:
        return self._cut_counts.get(proxy, 0)

    async def create(
        self, target: str, fault: str, params: Mapping[str, Any] | None = None
    ) -> Fault:
        normalized = normalize_params(fault, params)
        if target not in self._groups:
            raise UnknownGroupError(
                f"no link group {target!r}; GET /sim/links lists them (rosctl links)"
            )
        async with self._lock:
            for existing in self._faults.values():
                if existing.target == target and existing.fault == fault:
                    raise FaultExistsError(
                        f"{target} already has a {fault} fault ({existing.fault_id}); "
                        "heal it first",
                        existing.fault_id,
                    )
            fault_id = f"f{next(self._ids)}"
            proxies = self._groups[target]
            toxics: tuple[tuple[str, str], ...] = ()
            preexisting: tuple[str, ...] = ()
            if fault == "cut":
                preexisting = tuple(await self._cut(proxies))
            else:
                toxics = await self._add_toxics(fault_id, fault, normalized, proxies)
            record = Fault(
                fault_id=fault_id,
                target=target,
                fault=fault,
                params=normalized,
                proxies=proxies,
                created_at=datetime.now(UTC).isoformat(),
                toxics=toxics,
                preexisting=preexisting,
            )
            self._faults[fault_id] = record
            return record

    async def heal(self, fault_id: str) -> Healed:
        """Undo one fault. If Toxiproxy fails halfway, the fault is still
        forgotten (a retry could otherwise undo a shared cut twice) and
        the error says to run heal-all. The proxies a cut found already
        disabled stay disabled; their names come back in
        `Healed.left_disabled`."""
        async with self._lock:
            record = self._faults.pop(fault_id, None)
            if record is None:
                raise UnknownFaultError(
                    f"no fault {fault_id!r} in this control process (ids do not survive a "
                    "restart; heal-all clears what Toxiproxy still holds)"
                )
            try:
                if record.fault == "cut":
                    left_disabled = await self._uncut(record.proxies)
                else:
                    left_disabled = ()
                    for proxy, name in record.toxics:
                        await self._toxiproxy.remove_toxic(proxy, name)
            except ToxiproxyError as error:
                raise ToxiproxyError(
                    f"fault {fault_id} is only partly healed ({error}); run heal-all",
                    status=error.status,
                ) from error
            return Healed(fault=record, left_disabled=left_disabled)

    async def heal_all(self) -> int:
        """Toxiproxy's own reset: every proxy enabled, every toxic gone -
        including what no known fault explains. Returns how many known
        faults it cleared."""
        async with self._lock:
            await self._toxiproxy.reset()
            healed = len(self._faults)
            self._faults.clear()
            self._cut_counts.clear()
            self._pre_disabled.clear()
            return healed

    async def heal_all_if_alone(self) -> int | None:
        """`heal_all`, but only when no fault is tracked: a scenario's
        error cleanup must not wipe an operator's fault. Returns how many
        known faults it cleared (0 when there were none), or None when
        other faults are live and nothing was changed. The check and the
        reset share the layer's lock, so they cannot be split by another
        request."""
        async with self._lock:
            if self._faults:
                return None
            await self._toxiproxy.reset()
            healed = len(self._faults)
            self._faults.clear()
            self._cut_counts.clear()
            self._pre_disabled.clear()
            return healed

    async def links(self) -> list[dict[str, Any]]:
        """Every link group with its faults, plus what Toxiproxy holds on
        its proxies that no known fault explains (a restart forgets the
        ids, not the toxics; a cut leaves no toxic to rebuild it from)."""
        state = {str(entry.get("name")): entry for entry in await self._toxiproxy.list_proxies()}
        known_toxics = {pair for fault in self._faults.values() for pair in fault.toxics}
        answer: list[dict[str, Any]] = []
        for group, members in self._groups.items():
            disabled = [
                proxy
                for proxy in members
                if proxy in state
                and not bool(state[proxy].get("enabled", True))
                and self.cut_count(proxy) == 0
            ]
            stray_toxics = [
                {
                    "proxy": proxy,
                    "name": toxic.get("name"),
                    "type": toxic.get("type"),
                    "stream": toxic.get("stream"),
                }
                for proxy in members
                for toxic in (state.get(proxy, {}).get("toxics") or [])
                if (proxy, toxic.get("name")) not in known_toxics
            ]
            answer.append(
                {
                    "group": group,
                    "proxies": list(members),
                    "faults": [
                        fault.as_dict() for fault in self._faults.values() if fault.target == group
                    ],
                    # proxies a live cut holds that were already disabled
                    # when the cut arrived and will stay disabled on heal
                    "preexisting_disabled": [
                        proxy for proxy in members if proxy in self._pre_disabled
                    ],
                    "unexplained": {
                        "disabled": disabled,
                        "toxics": stray_toxics,
                        "missing": [proxy for proxy in members if proxy not in state],
                    },
                }
            )
        return answer

    # --- the Toxiproxy changes, each rolled back if it fails halfway -------

    async def _cut(self, proxies: Sequence[str]) -> list[str]:
        """Disable the group's proxies. Returns the proxies that were
        ALREADY disabled (no request is sent for them): they belong to
        someone else - a leftover from an earlier control process, or a
        manual change - and a heal must leave them as they are."""
        state: dict[str, dict[str, Any]] | None = None
        preexisting: list[str] = []
        flagged: list[str] = []
        counted: list[str] = []
        disabled: list[str] = []
        try:
            for proxy in proxies:
                if self.cut_count(proxy) == 0:
                    if state is None:  # one read of the live state
                        state = {
                            str(entry.get("name")): entry
                            for entry in await self._toxiproxy.list_proxies()
                        }
                    already = not bool(state.get(proxy, {}).get("enabled", True))
                    if already:
                        self._pre_disabled.add(proxy)
                        preexisting.append(proxy)
                        flagged.append(proxy)
                    else:
                        await self._toxiproxy.set_enabled(proxy, False)
                        disabled.append(proxy)
                self._cut_counts[proxy] = self.cut_count(proxy) + 1
                counted.append(proxy)
        except BaseException:
            for proxy in counted:
                if self._decrement(proxy) == 0 and proxy in flagged:
                    self._pre_disabled.discard(proxy)  # this call created the flag
            for proxy in disabled:  # only what this call flipped
                with contextlib.suppress(ToxiproxyError):
                    await self._toxiproxy.set_enabled(proxy, True)
            raise
        return preexisting

    async def _uncut(self, proxies: Sequence[str]) -> tuple[str, ...]:
        """Undo one cut. Pass 1 drops this fault's hold on every proxy
        (local bookkeeping only); pass 2 enables the proxies whose count
        reached zero AND that this layer disabled itself. A proxy that
        was already disabled before its first cut keeps its state: it
        lands in the returned `left_disabled`. An enable that fails
        leaves the count at zero - the proxy then shows up in `links()`
        as disabled with no owner - but every proxy is still tried, and
        the first failure surfaces after the loop."""
        left_disabled: list[str] = []
        to_enable: list[str] = []
        for proxy in proxies:
            if self._decrement(proxy) != 0:
                continue  # another live cut still holds the proxy
            if proxy in self._pre_disabled:
                self._pre_disabled.discard(proxy)
                left_disabled.append(proxy)
            else:
                to_enable.append(proxy)
        failure: ToxiproxyError | None = None
        for proxy in to_enable:
            try:
                await self._toxiproxy.set_enabled(proxy, True)
            except ToxiproxyError as error:
                if failure is None:
                    failure = error
        if failure is not None:
            raise failure
        return tuple(left_disabled)

    def _decrement(self, proxy: str) -> int:
        left = max(self.cut_count(proxy) - 1, 0)
        if left:
            self._cut_counts[proxy] = left
        else:
            self._cut_counts.pop(proxy, None)
        return left

    async def _add_toxics(
        self, fault_id: str, fault: str, params: Mapping[str, int], proxies: Sequence[str]
    ) -> tuple[tuple[str, str], ...]:
        toxic_type = TOXIC_TYPES[fault]
        attributes = toxic_attributes(fault, params)
        added: list[tuple[str, str]] = []
        try:
            for proxy in proxies:
                for suffix, stream in _DIRECTIONS:
                    name = f"{fault_id}-{fault}-{suffix}"
                    await self._toxiproxy.add_toxic(
                        proxy, toxic_type, attributes, stream=stream, name=name
                    )
                    added.append((proxy, name))
        except BaseException:
            for proxy, name in added:
                with contextlib.suppress(ToxiproxyError):
                    await self._toxiproxy.remove_toxic(proxy, name)
            raise
        return tuple(added)
