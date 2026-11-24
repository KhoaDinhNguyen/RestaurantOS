"""The `skip_first_restock` start flag: a restored day is replayed without
the midnight restock that already went out before the snapshot."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
from conftest import FakeWallClock, drive, make_order, make_playback

from restaurantos.sim.control import make_control_app


def _restock_days_logged(runs_dir: Path, run_id: str) -> list[int]:
    lines = (runs_dir / run_id / "orders.jsonl").read_text().splitlines()
    return [
        record["restock_day"] for record in map(json.loads, lines) if record["kind"] == "restock"
    ]


def _engine(tmp_path: Path, clock: FakeWallClock):
    orders = [make_order(21, 9, 0, 0, "1001"), make_order(28, 9, 0, 0, "1002")]
    return make_playback(
        orders,
        tmp_path,
        clock=clock,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001",),
    )


async def test_by_default_the_from_day_restock_goes_out(tmp_path: Path) -> None:
    clock = FakeWallClock()
    pb = _engine(tmp_path, clock)
    await pb.start(seed=0, from_day=21, to_day=28, speed=100000.0, run_id="plain")
    await drive(pb, clock)
    assert _restock_days_logged(tmp_path, "plain") == [21, 28]
    meta = json.loads((tmp_path / "plain" / "meta.json").read_text())
    assert meta["restock"]["skipped_first"] is False


async def test_the_flag_drops_only_the_from_day_restock(tmp_path: Path) -> None:
    clock = FakeWallClock()
    pb = _engine(tmp_path, clock)
    await pb.start(
        seed=0,
        from_day=21,
        to_day=28,
        speed=100000.0,
        run_id="resumed",
        skip_first_restock=True,
    )
    await drive(pb, clock)
    assert _restock_days_logged(tmp_path, "resumed") == [28]
    meta = json.loads((tmp_path / "resumed" / "meta.json").read_text())
    assert meta["restock"]["days"] == [28]
    assert meta["restock"]["skipped_first"] is True


async def test_the_flag_on_a_day_without_restock_changes_nothing(tmp_path: Path) -> None:
    clock = FakeWallClock()
    pb = make_playback(
        [make_order(22, 9, 0, 0, "2001"), make_order(28, 9, 0, 0, "2002")],
        tmp_path,
        clock=clock,
        restock_every=7,
        warm_up_days=14,
        restock_vendors=("v001",),
    )
    await pb.start(
        seed=0, from_day=22, to_day=28, speed=100000.0, run_id="d22", skip_first_restock=True
    )
    await drive(pb, clock)
    assert _restock_days_logged(tmp_path, "d22") == [28]


async def test_the_start_body_carries_the_flag(tmp_path: Path) -> None:
    clock = FakeWallClock()
    pb = _engine(tmp_path, clock)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_control_app(pb)), base_url="http://control.test"
    ) as client:
        response = await client.post(
            "/sim/playback",
            json={
                "action": "start",
                "from_day": 21,
                "to_day": 28,
                "speed": 100000.0,
                "run_id": "via-api",
                "skip_first_restock": True,
            },
        )
        assert response.status_code == 200
    await drive(pb, clock)
    assert _restock_days_logged(tmp_path, "via-api") == [28]
