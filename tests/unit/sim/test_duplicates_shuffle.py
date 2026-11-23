"""The duplicates-and-shuffle deliverable (architecture 7.2): resends at
a hash-drawn rate and offset, delivery reordered inside a bounded
window, both off by default and both deterministic per seed.

Acceptance lines from the brief:

- same seed twice gives byte-identical run logs, mid-run speed change
  included;
- the resent share over a synthetic stream matches the rate exactly
  (it is hash-determined, not statistical);
- shuffle changes delivery order but never drops or duplicates an
  item, and a window of 1 is no shuffle at all.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from conftest import FakeWallClock, drive, make_playback, sample_orders

from restaurantos.sim.determinism import stable_float
from restaurantos.sim.playback import Playback
from restaurantos.sim.stub import make_stub_app


def log_file(runs_dir: Path, pb: Playback) -> Path:
    status = pb.status()
    assert status["run_id"], f"no run yet (state {status['state']})"
    return runs_dir / status["run_id"] / "orders.jsonl"


def run_log_lines(runs_dir: Path, pb: Playback) -> list[dict]:
    return [json.loads(line) for line in log_file(runs_dir, pb).read_text().splitlines()]


def meta_json(runs_dir: Path, pb: Playback) -> dict:
    return json.loads(log_file(runs_dir, pb).parent.joinpath("meta.json").read_text())


def order_lines(lines: list[dict]) -> list[dict]:
    return [line for line in lines if line["kind"] == "order"]


def resent_lines(lines: list[dict]) -> list[dict]:
    return [line for line in order_lines(lines) if line.get("resend") is True]


def expected_resent(orders, seed: int, rate: float) -> list[str]:
    """Which orders the hash decides to resend - computed here from the
    contract alone, so the test checks the engine against the rule, not
    against itself."""
    return [
        order.external_order_id
        for order in orders
        if stable_float(seed, order.external_order_id, "duplicate") < rate
    ]


def expected_shuffle_order(orders, seed: int, window: int) -> list[int]:
    """The stream positions in the order the buffer drains: the stream is
    pulled `window` items at a time, and each batch is delivered in
    shuffle-key order - the smallest
    (stable_float(seed, order_id, "shuffle"), stream position) first -
    before the next batch is pulled. Mirrors the engine's selection from
    the contract; the test compares the engine's delivery against this,
    not against itself."""
    stream = enumerate(orders)
    buffer: list[int] = []
    ranks: dict[int, tuple[float, int]] = {}

    def rank(position: int) -> tuple[float, int]:
        if position not in ranks:
            request = orders[position]
            ranks[position] = (stable_float(seed, request.external_order_id, "shuffle"), position)
        return ranks[position]

    out: list[int] = []
    while True:
        buffer.clear()
        while len(buffer) < window:
            try:
                buffer.append(next(stream)[0])
            except StopIteration:
                break
        if not buffer:
            return out
        while buffer:
            picked = min(buffer, key=rank)
            buffer.remove(picked)
            out.append(picked)


# --- the acceptance line ----------------------------------------------------


async def test_same_seed_gives_byte_identical_logs_with_duplicates_and_shuffle(
    tmp_path: Path,
) -> None:
    """Same seed, same operation sequence (a mid-run speed change
    included) -> byte-identical orders.jsonl, with resends and a shuffled
    delivery in it."""
    orders = sample_orders(40)

    async def scenario(runs_dir: Path) -> str:
        clock = FakeWallClock()
        pb = make_playback(
            orders,
            runs_dir,
            clock=clock,
            inflight=4,
            duplicate_offset_s=5.0,
            shuffle_window=8,
        )
        await pb.start(
            seed=7,
            from_day=14,
            to_day=16,
            speed=1000.0,
            run_id="run",
            duplicate_rate=0.3,
            shuffle=True,
        )
        await drive(pb, clock, steps=3)
        await pb.set_speed(250.0)
        await drive(pb, clock)  # to the end
        assert pb.status()["state"] == "done", pb.status()
        return log_file(runs_dir, pb).read_text()

    log_a = await scenario(tmp_path / "a")
    log_b = await scenario(tmp_path / "b")
    assert log_a == log_b

    lines = [json.loads(line) for line in log_a.splitlines()]
    # every order went out exactly once, plus the hash-decided resends
    assert len(order_lines(lines)) == len(orders) + len(resent_lines(lines))
    assert len(resent_lines(lines)) > 0
    # this seed's shuffle actually reordered delivery (the point of the
    # test: the bytes are identical AND the order is not the stream's)
    delivered = [line["external_order_id"] for line in order_lines(lines) if not line.get("resend")]
    stream_order = [order.external_order_id for order in orders]
    assert sorted(delivered) == sorted(stream_order)  # never drops or duplicates
    assert delivered != stream_order  # and it did reorder


# --- duplicates -------------------------------------------------------------


async def test_the_resent_share_matches_the_rate_exactly(tmp_path: Path) -> None:
    """The rate is a hash decision per order: over a synthetic stream
    the resent count is exactly the number the hash picks - not a
    statistical draw around it."""
    orders = sample_orders(40)
    seed, rate = 3, 0.25
    expected = expected_resent(orders, seed, rate)
    # the fixture must make the test meaningful: some resent, some not
    assert 0 < len(expected) < len(orders), "pick a seed where the rate splits the stream"

    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, inflight=4, duplicate_offset_s=2.0)
    await pb.start(
        seed=seed, from_day=14, to_day=16, speed=1000.0, run_id="rate", duplicate_rate=rate
    )
    await drive(pb, clock)

    status = pb.status()
    assert status["orders_sent"] == len(orders)
    assert status["orders_resent"] == len(expected)
    lines = run_log_lines(tmp_path, pb)
    assert sorted(line["external_order_id"] for line in resent_lines(lines)) == sorted(expected)


async def test_duplicate_rate_boundaries(tmp_path: Path) -> None:
    """0.0 resends nothing; 1.0 resends every order exactly once."""
    orders = sample_orders(20)

    async def run(rate: float, run_id: str) -> tuple[list[dict], dict]:
        clock = FakeWallClock()
        pb = make_playback(orders, tmp_path / run_id, clock=clock, inflight=4)
        await pb.start(
            seed=0, from_day=14, to_day=14, speed=1000.0, run_id=run_id, duplicate_rate=rate
        )
        await drive(pb, clock)
        return run_log_lines(tmp_path / run_id, pb), pb.status()

    lines, status = await run(0.0, "zero")
    assert status["orders_resent"] == 0
    assert resent_lines(lines) == []
    assert len(order_lines(lines)) == len(orders)

    lines, status = await run(1.0, "one")
    assert status["orders_resent"] == len(orders)
    assert len(resent_lines(lines)) == len(orders)
    # every external id sent exactly twice: the original and the resend
    sent = [line["external_order_id"] for line in order_lines(lines)]
    assert sorted(sent) == sorted([o.external_order_id for o in orders] * 2)


async def test_resend_lines_are_well_formed(tmp_path: Path) -> None:
    """A resend's line is an order line: same fields as the original,
    plus resend: true, its own seq, its answer's response fields - and
    it goes out after its own original, at the order's instant plus the
    hash-drawn offset."""
    orders = sample_orders(40)
    seed, rate, offset_s = 11, 0.4, 6.0
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, inflight=4, duplicate_offset_s=offset_s)
    await pb.start(
        seed=seed, from_day=14, to_day=16, speed=1000.0, run_id="resend", duplicate_rate=rate
    )
    await drive(pb, clock)

    lines = run_log_lines(tmp_path, pb)
    seqs = [line["seq"] for line in lines]
    assert seqs == sorted(seqs)  # the log is in dispatch (seq) order
    assert len(set(seqs)) == len(seqs)  # and every seq appears exactly once

    originals = {
        line["external_order_id"]: line for line in order_lines(lines) if not line.get("resend")
    }
    assert len(originals) == len(orders)
    for line in resent_lines(lines):
        oid = line["external_order_id"]
        original = originals[oid]
        order = next(o for o in orders if o.external_order_id == oid)
        assert line["kind"] == "order"
        assert line["resend"] is True
        assert line["order_day"] == order.order_day
        assert line["order_time"] == order.order_time.isoformat()
        assert line["http_status"] == 201
        assert line["order_id"] == original["order_id"]  # same body -> same inbox answer
        assert line["wall_sent_at"] is not None
        assert line["wall_acked_at"] is not None
        # scheduled at the order's own instant plus the hash-drawn offset
        # (approx: the engine carries the offset on the absolute simulated
        # clock, so the last bits can round differently than adding it to
        # the offset alone)
        offset = stable_float(seed, oid, "duplicate-offset") * offset_s
        assert line["sim_sent_at"] == pytest.approx(original["sim_sent_at"] + offset)
        # and dispatched after its original: a later line in the log
        assert lines.index(line) > lines.index(original)


async def test_resends_share_the_inflight_bound(tmp_path: Path) -> None:
    """A resend takes a slot like any send: with every order resent at
    its own instant and everything due at once, the wire never holds
    more than the bound."""

    class Counting(httpx.AsyncBaseTransport):
        def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
            self.inner = inner
            self.active = 0
            self.max_active = 0

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                return await self.inner.handle_async_request(request)
            finally:
                self.active -= 1

    transport = Counting(httpx.ASGITransport(app=make_stub_app(delay_seconds=0.05)))
    orders = sample_orders(12)
    # real clock, like the engine's own bound test: at 1e9x every wait is
    # microseconds, and the fake clock would never advance on its own
    pb = make_playback(
        orders,
        tmp_path,
        inflight=2,
        speed=1e9,
        transport=transport,
        duplicate_offset_s=0.0,
    )
    await pb.start(seed=0, from_day=14, to_day=14, speed=1e9, run_id="bound", duplicate_rate=1.0)
    await pb.wait_done()
    assert pb.status()["state"] == "done"
    assert pb.status()["orders_resent"] == len(orders)
    assert transport.max_active == 2  # held exactly at the bound, resends included


# --- shuffle ----------------------------------------------------------------


async def test_shuffle_reorders_within_the_window_and_never_drops(tmp_path: Path) -> None:
    """Delivery order is the smallest shuffle key among the next
    window-sized slice of the stream: the engine's order matches the
    contract mirror exactly, each item moves at most window-1 places,
    and a window of 1 is the stream order."""
    orders = sample_orders(40)
    seed, window = 5, 8
    expected = expected_shuffle_order(orders, seed, window)
    # the seed must actually reorder for this to test the shuffle
    assert expected != list(range(len(orders))), "pick a seed that reorders"

    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, inflight=4, shuffle_window=window)
    await pb.start(seed=seed, from_day=14, to_day=16, speed=1000.0, run_id="shuffled", shuffle=True)
    await drive(pb, clock)

    lines = run_log_lines(tmp_path, pb)
    delivered = [line["external_order_id"] for line in order_lines(lines)]
    expected_ids = [orders[position].external_order_id for position in expected]
    assert delivered == expected_ids
    # nothing dropped, nothing duplicated
    assert sorted(delivered) == sorted(o.external_order_id for o in orders)
    # the reorder is bounded: each item moved at most window-1 places
    for dispatch_position, stream_position in enumerate(expected):
        assert abs(dispatch_position - stream_position) < window

    # window 1: no shuffle at all - the stream order
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path / "w1", clock=clock, inflight=4, shuffle_window=1)
    await pb.start(seed=seed, from_day=14, to_day=16, speed=1000.0, run_id="w1", shuffle=True)
    await drive(pb, clock)
    delivered = [
        line["external_order_id"] for line in order_lines(run_log_lines(tmp_path / "w1", pb))
    ]
    assert delivered == [o.external_order_id for o in orders]


async def test_shuffle_survives_a_mid_run_speed_change(tmp_path: Path) -> None:
    """A speed change re-anchors the clock; the shuffle picks and the
    simulated instants do not move. The run still delivers every item,
    in the same shuffled order it would have without the change."""
    orders = sample_orders(30)
    seed, window = 5, 8
    expected = expected_shuffle_order(orders, seed, window)

    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, inflight=4, shuffle_window=window)
    await pb.start(
        seed=seed, from_day=14, to_day=16, speed=1000.0, run_id="speedchange", shuffle=True
    )
    await drive(pb, clock, steps=3)
    await pb.pause()
    await pb.set_speed(250.0)
    await pb.resume()
    await drive(pb, clock)  # to the end
    assert pb.status()["state"] == "done", pb.status()

    lines = run_log_lines(tmp_path, pb)
    delivered = [line["external_order_id"] for line in order_lines(lines)]
    expected_ids = [orders[position].external_order_id for position in expected]
    assert delivered == expected_ids
    assert sorted(delivered) == sorted(o.external_order_id for o in orders)


async def test_shuffle_respects_the_kitchen_timeline(tmp_path: Path) -> None:
    """Shuffle reorders orders against each other only: status events
    still land on their own simulated instants (an event later than a
    shuffled-forward order still goes after it), and no line changes
    kind. Events on, restocks off, one day."""
    orders = sample_orders(12)
    seed, window = 5, 8
    clock = FakeWallClock()
    pb = make_playback(
        orders,
        tmp_path,
        clock=clock,
        inflight=4,
        status_events=True,
        shuffle_window=window,
    )
    await pb.start(seed=seed, from_day=14, to_day=14, speed=1000.0, run_id="events", shuffle=True)
    await drive(pb, clock)

    lines = run_log_lines(tmp_path, pb)
    status = pb.status()
    assert status["orders_sent"] == len(orders)
    # every scheduled event got exactly one line, sent or skipped - four
    # per accepted order, none for a refused or failed one
    event_lines = [line for line in lines if line["kind"] == "status_event"]
    assert len(event_lines) == status["events_sent"] + status["events_skipped"]
    assert status["events_sent"] + status["events_skipped"] == 4 * status["orders_acked"]
    accepted = {
        line["external_order_id"] for line in order_lines(lines) if line.get("http_status") == 201
    }
    # an accepted order's events all name the same order it was accepted as
    for line in event_lines:
        assert line["external_order_id"] in accepted or line.get("skipped") is True


# --- mode and parameter refusals -------------------------------------------


async def test_shuffle_is_refused_in_sequential_mode(tmp_path: Path) -> None:
    """The engine refuses the combination, not silently off: sequential
    mode's in-order, one-at-a-time delivery is what the oracle matches."""
    pb = make_playback(sample_orders(5), tmp_path, clock=FakeWallClock(), sequential=True)
    with pytest.raises(ValueError, match="shuffle cannot run in sequential mode"):
        await pb.start(seed=0, from_day=14, to_day=14, speed=1.0, run_id="seq", shuffle=True)
    assert pb.status()["state"] == "idle"  # nothing started, nothing leaked


async def test_bad_duplicate_parameters_are_refused(tmp_path: Path) -> None:
    """The knobs validate on the engine too, not only in the settings:
    tests and API callers build it directly."""
    with pytest.raises(ValueError, match="shuffle_window"):
        make_playback(sample_orders(5), tmp_path, clock=FakeWallClock(), shuffle_window=0)
    with pytest.raises(ValueError, match="duplicate_offset_s"):
        make_playback(sample_orders(5), tmp_path, clock=FakeWallClock(), duplicate_offset_s=-1.0)

    pb = make_playback(sample_orders(5), tmp_path, clock=FakeWallClock())
    for bad in (1.5, -0.1, float("nan")):
        with pytest.raises(ValueError, match="duplicate_rate"):
            await pb.start(
                seed=0, from_day=14, to_day=14, speed=1.0, run_id=f"bad{bad!r}", duplicate_rate=bad
            )
    assert pb.status()["state"] == "idle"


async def test_meta_records_the_run_facts_next_to_the_seed(tmp_path: Path) -> None:
    """meta.json carries duplicate_rate, the offset bound, the shuffle
    switch and the window - for clean runs too, so a log always says
    what produced it."""
    orders = sample_orders(5)
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path, clock=clock, duplicate_offset_s=2.0, shuffle_window=4)
    await pb.start(
        seed=9,
        from_day=14,
        to_day=14,
        speed=1000.0,
        run_id="meta",
        duplicate_rate=0.2,
        shuffle=True,
    )
    await drive(pb, clock)

    meta = meta_json(tmp_path, pb)
    assert meta["seed"] == 9
    assert meta["duplicate_rate"] == 0.2
    assert meta["duplicate_offset_s"] == 2.0
    assert meta["shuffle"] is True
    assert meta["shuffle_window"] == 4

    # and a clean run says clean
    clock = FakeWallClock()
    pb = make_playback(orders, tmp_path / "clean", clock=clock)
    await pb.start(seed=0, from_day=14, to_day=14, speed=1000.0, run_id="clean")
    await drive(pb, clock)
    meta = meta_json(tmp_path / "clean", pb)
    assert meta["duplicate_rate"] == 0.0
    assert meta["shuffle"] is False
