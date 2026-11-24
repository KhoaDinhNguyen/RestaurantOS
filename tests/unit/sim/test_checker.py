"""The invariant checker's evaluation on fake run logs and fake facts:
every mismatch kind, lost at failover against unaccounted, duplicates,
parked and refused orders, and stored_unacked. No database."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from restaurantos.common.cluster import Cluster
from restaurantos.sim.checker import (
    RESUMED_FROM,
    CheckError,
    ClusterFacts,
    HoldingRow,
    evaluate,
    format_report,
    read_runs,
    refuse_edge_runs,
    run_chain,
)

CH = "dhrd"
T0 = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def accepted(ext: str, *, region: str = "r1", minute: float = 0.0, resend: bool = False) -> dict:
    line = {
        "kind": "order",
        "external_order_id": ext,
        "http_status": 200,
        "order_id": f"uuid-{ext}",
        "region_id": region,
        "acked_at": at(minute).isoformat(),
    }
    if resend:
        line["resend"] = True
    return line


def parked(ext: str) -> dict:
    return {"kind": "order", "external_order_id": ext, "http_status": 202, "queued": True}


def refused(ext: str, region: str = "r1") -> dict:
    return {
        "kind": "order",
        "external_order_id": ext,
        "http_status": 409,
        "region_id": region,
        "refusal_reason": "out_of_stock",
    }


def failed(ext: str, *, sent_minute: float | None = None) -> dict:
    line: dict = {"kind": "order", "external_order_id": ext, "error": "Timeout"}
    if sent_minute is not None:
        # wall_sent_at is on the engine's monotonic scale; write_run's
        # default meta puts monotonic 0 at T0
        line["wall_sent_at"] = sent_minute * 60.0
    return line


def write_run(
    runs: Path, run_id: str, lines: list[dict], *, parent: str | None = None, **meta
) -> None:
    run_dir = runs / run_id
    run_dir.mkdir(parents=True)
    body = {
        "run_id": run_id,
        "from_day": 14,
        "to_day": 20,
        "wall_start_epoch": T0.timestamp(),
        "wall_start_monotonic": 0.0,
        **meta,
    }
    (run_dir / "meta.json").write_text(json.dumps(body), encoding="utf-8")
    events = [{"kind": "status_event", "order_id": "x"}, {"kind": "restock", "restock_day": 21}]
    text = "\n".join(json.dumps(line) for line in [*lines, *events]) + "\n"
    (run_dir / "orders.jsonl").write_text(text, encoding="utf-8")
    if parent is not None:
        (run_dir / RESUMED_FROM).write_text(json.dumps({"parent_run": parent}), encoding="utf-8")


def facts(
    stored: dict[str, list[str]] | None = None,
    hq: list[tuple[str, str]] | None = None,
    **kwargs,
) -> ClusterFacts:
    return ClusterFacts(
        stored={region: [(CH, ext) for ext in exts] for region, exts in (stored or {}).items()},
        hq=[(CH, ext, region) for ext, region in (hq or [])],
        refused=kwargs.pop("refused", set()),
        holding=kwargs.pop("holding", {}),
        **kwargs,
    )


def check(tmp_path: Path, lines: list[dict], cluster: ClusterFacts, **kwargs) -> dict:
    write_run(tmp_path, "run", lines)
    return evaluate(read_runs(tmp_path, ["run"], CH), cluster, **kwargs)


def test_a_clean_run_passes(tmp_path) -> None:
    result = check(
        tmp_path,
        [accepted("1"), accepted("2"), refused("3")],
        facts({"r1": ["1", "2"]}, [("1", "r1"), ("2", "r1")], refused={(CH, "3")}),
    )
    assert result["passed"] is True
    assert (result["acked"], result["stored"], result["hq"]) == (2, 2, 2)
    assert result["lost_at_failover"] == {"count": 0, "order_ids": []}
    assert result["duplicates"] == [] and result["unaccounted"] == []
    assert result["stored_unacked"] == {"count": 0, "order_ids": [], "allowed": False}
    assert set(result) >= {
        "passed",
        "acked",
        "stored",
        "hq",
        "lost_at_failover",
        "duplicates",
        "unaccounted",
        "stored_unacked",
    }


def test_a_deleted_order_is_unaccounted_without_a_failover(tmp_path) -> None:
    """The done-when line: hand-DELETE an order from the region."""
    result = check(
        tmp_path,
        [accepted("1"), accepted("2")],
        facts({"r1": ["1"]}, [("1", "r1"), ("2", "r1")]),
    )
    assert result["passed"] is False
    assert result["unaccounted"] == [
        {"external_order_id": "2", "reason": "acked_not_stored", "region_id": "r1"}
    ]
    assert result["lost_at_failover"]["count"] == 0


def test_a_duplicate_inside_one_region_fails(tmp_path) -> None:
    """The done-when line: hand-INSERT a duplicate external_order_id."""
    result = check(
        tmp_path,
        [accepted("1")],
        facts({"r1": ["1", "1"]}, [("1", "r1")]),
    )
    assert result["passed"] is False
    assert result["duplicates"] == [
        {"external_order_id": "1", "where": "regions", "regions": ["r1", "r1"]}
    ]


def test_a_duplicate_across_regions_and_at_hq_fails(tmp_path) -> None:
    result = check(
        tmp_path,
        [accepted("1")],
        facts({"r1": ["1"], "r2": ["1"]}, [("1", "r1"), ("1", "r2")]),
    )
    assert {entry["where"] for entry in result["duplicates"]} == {"regions", "hq"}


def test_lost_after_an_async_failover_is_reported_not_failed(tmp_path) -> None:
    result = check(
        tmp_path,
        [accepted("1", minute=0), accepted("2", minute=1)],
        facts({"r1": ["1"]}, [("1", "r1"), ("2", "r1")], failovers=[("r1", at(5))]),
    )
    assert result["passed"] is True
    assert result["lost_at_failover"] == {"count": 1, "order_ids": ["2"]}


def test_a_failover_before_the_ack_does_not_explain_a_missing_order(tmp_path) -> None:
    result = check(
        tmp_path,
        [accepted("1", minute=10)],
        facts({"r1": []}, [("1", "r1")], failovers=[("r1", at(5))]),
    )
    assert result["passed"] is False
    assert result["unaccounted"][0]["reason"] == "acked_not_stored"


def test_the_latest_ack_bounds_a_loss(tmp_path) -> None:
    """Acked at 0 and again at 10 (a resend): the second answer proves the
    region still had the order after a failover at 5, so that failover
    cannot explain it going missing later."""
    lines = [accepted("1", minute=0), accepted("1", minute=10, resend=True)]
    result = check(tmp_path, lines, facts({"r1": []}, [("1", "r1")], failovers=[("r1", at(5))]))
    assert result["passed"] is False
    assert result["unaccounted"][0]["reason"] == "acked_not_stored"
    assert result["acked"] == 1  # still one order


def test_a_failover_after_the_latest_ack_still_explains_a_loss(tmp_path) -> None:
    lines = [accepted("1", minute=10, resend=True), accepted("1", minute=0)]
    result = check(tmp_path, lines, facts({"r1": []}, [("1", "r1")], failovers=[("r1", at(15))]))
    assert result["passed"] is True
    assert result["lost_at_failover"]["order_ids"] == ["1"]


def test_a_failover_of_another_region_does_not_explain_it(tmp_path) -> None:
    result = check(
        tmp_path,
        [accepted("1", region="r1")],
        facts({"r1": []}, [("1", "r1")], failovers=[("r2", at(5))]),
    )
    assert result["passed"] is False


def test_with_synchronous_replication_a_missing_order_is_unaccounted(tmp_path) -> None:
    result = check(
        tmp_path,
        [accepted("1")],
        facts({"r1": []}, [("1", "r1")], failovers=[("r1", at(5))], synchronous=True),
    )
    assert result["passed"] is False
    assert result["lost_at_failover"]["count"] == 0


def test_a_sync_region_downgraded_before_the_failover_may_lose_orders(tmp_path) -> None:
    result = check(
        tmp_path,
        [accepted("1")],
        facts(
            {"r1": []},
            [("1", "r1")],
            failovers=[("r1", at(5))],
            downgrades=[("r1", at(3))],
            synchronous=True,
        ),
    )
    assert result["passed"] is True
    assert result["lost_at_failover"]["order_ids"] == ["1"]


def test_an_hq_only_failed_order_needs_a_failover_after_its_first_send(tmp_path) -> None:
    """Every send errored, HQ has the order, its region does not: only a
    failover after the order was first sent can have lost it."""
    cluster = facts({"r1": []}, [("1", "r1")], failovers=[("r1", at(5))])
    late = check(tmp_path / "late", [failed("1", sent_minute=10)], cluster)
    assert late["passed"] is False
    assert late["unaccounted"][0]["reason"] == "in_hq_not_in_region"
    early = check(tmp_path / "early", [failed("1", sent_minute=1)], cluster)
    assert early["passed"] is True
    assert early["lost_at_failover"]["order_ids"] == ["1"]


def test_an_hq_only_failed_order_without_a_send_time_is_unaccounted(tmp_path) -> None:
    result = check(
        tmp_path, [failed("1")], facts({"r1": []}, [("1", "r1")], failovers=[("r1", at(5))])
    )
    assert result["passed"] is False
    assert result["unaccounted"][0]["reason"] == "in_hq_not_in_region"


def test_in_region_but_not_at_hq_after_the_drain_is_a_relay_bug(tmp_path) -> None:
    result = check(tmp_path, [accepted("1")], facts({"r1": ["1"]}, []))
    assert result["unaccounted"] == [
        {"external_order_id": "1", "reason": "stored_not_in_hq", "region_id": "r1"}
    ]


def test_parked_orders_follow_their_holding_row(tmp_path) -> None:
    holding = {
        (CH, "p1"): HoldingRow("r1", at(1), "accepted"),
        (CH, "p2"): HoldingRow("r1", at(1), "refused"),
    }
    result = check(
        tmp_path,
        [parked("p1"), parked("p2")],
        facts({"r1": ["p1"]}, [("p1", "r1")], holding=holding, refused={(CH, "p2")}),
    )
    assert result["passed"] is True
    assert result["acked"] == 1


def test_a_parked_order_without_a_holding_row_or_undelivered_is_unaccounted(tmp_path) -> None:
    holding = {(CH, "p2"): HoldingRow("r1", None, None)}
    result = check(tmp_path, [parked("p1"), parked("p2")], facts({"r1": []}, [], holding=holding))
    reasons = {entry["external_order_id"]: entry["reason"] for entry in result["unaccounted"]}
    assert reasons == {"p1": "parked_without_holding_row", "p2": "parked_not_delivered"}


def test_a_refused_order_missing_from_the_inbox_is_unaccounted(tmp_path) -> None:
    result = check(tmp_path, [refused("3")], facts({"r1": []}, []))
    assert result["unaccounted"][0]["reason"] == "refused_not_in_inbox"


def test_a_refused_order_that_is_stored_is_unaccounted(tmp_path) -> None:
    result = check(
        tmp_path, [refused("3")], facts({"r1": ["3"]}, [("3", "r1")], refused={(CH, "3")})
    )
    assert result["unaccounted"][0]["reason"] == "refused_but_stored"


def test_stored_unacked_fails_by_default_and_passes_when_allowed(tmp_path) -> None:
    cluster = facts({"r1": ["1", "2"]}, [("1", "r1"), ("2", "r1")])
    strict = check(tmp_path, [accepted("1"), failed("2")], cluster)
    assert strict["passed"] is False
    assert strict["stored_unacked"] == {"count": 1, "order_ids": ["2"], "allowed": False}
    assert strict["unaccounted"] == []
    allowed = evaluate(read_runs(tmp_path, ["run"], CH), cluster, allow_unacked=True)
    assert allowed["passed"] is True
    assert allowed["stored_unacked"]["allowed"] is True


def test_a_failed_order_that_never_landed_is_fine(tmp_path) -> None:
    result = check(tmp_path, [accepted("1"), failed("2")], facts({"r1": ["1"]}, [("1", "r1")]))
    assert result["passed"] is True


def test_resends_count_once_and_the_best_outcome_wins(tmp_path) -> None:
    lines = [failed("1"), accepted("1", resend=True), accepted("2"), accepted("2", resend=True)]
    result = check(tmp_path, lines, facts({"r1": ["1", "2"]}, [("1", "r1"), ("2", "r1")]))
    assert result["passed"] is True and result["acked"] == 2


def test_a_stored_order_from_outside_the_run_is_unaccounted(tmp_path) -> None:
    result = check(tmp_path, [accepted("1")], facts({"r1": ["1", "9"]}, [("1", "r1"), ("9", "r1")]))
    assert result["unaccounted"] == [
        {"external_order_id": "9", "reason": "stored_not_in_run", "region_id": "r1"}
    ]


def test_a_restored_run_follows_its_parent(tmp_path) -> None:
    write_run(tmp_path, "a", [accepted("1"), accepted("2")])
    write_run(tmp_path, "a-r1", [accepted("2"), accepted("3")], parent="a", from_day=16)
    assert run_chain(tmp_path, ["a-r1"]) == ["a", "a-r1"]
    runs = read_runs(tmp_path, ["a-r1"], CH)
    assert runs.run_ids == ["a", "a-r1"]
    assert set(runs.orders) == {(CH, "1"), (CH, "2"), (CH, "3")}
    assert (runs.from_day, runs.to_day) == (14, 20)


def test_runs_that_disagree_on_the_region_are_refused(tmp_path) -> None:
    write_run(tmp_path, "a", [], region="r1")
    write_run(tmp_path, "b", [], region="r2")
    with pytest.raises(CheckError, match="disagree"):
        read_runs(tmp_path, ["a", "b"], CH)


def test_an_edge_run_is_refused_and_pointed_at_the_oracle(tmp_path) -> None:
    from restaurantos.sim.playback_cli import edge_target_url

    cluster = Cluster(regions=1)
    write_run(
        tmp_path,
        "edge",
        [],
        region="r1",
        target_url=edge_target_url(cluster, "r1"),
    )
    runs = read_runs(tmp_path, ["edge"], CH)
    with pytest.raises(CheckError, match="oracle compare"):
        refuse_edge_runs(cluster, runs)
    # a gateway run of the same region is what the checker is for
    write_run(
        tmp_path,
        "gateway",
        [],
        region="r1",
        target_url="http://127.0.0.1:8000/api/v1",
    )
    refuse_edge_runs(cluster, read_runs(tmp_path, ["gateway"], CH))


def test_a_missing_run_is_a_check_error(tmp_path) -> None:
    with pytest.raises(CheckError, match="run nope"):
        read_runs(tmp_path, ["nope"], CH)


def test_the_report_names_every_difference(tmp_path) -> None:
    result = check(
        tmp_path,
        [accepted("1"), accepted("2"), failed("4")],
        facts({"r1": ["1", "1", "4"]}, [("1", "r1"), ("4", "r1")]),
    )
    report = format_report(result)
    assert report.startswith("check: FAILED")
    assert "2 acked_not_stored (r1)" in report
    assert "1 (regions: ['r1', 'r1'])" in report
    assert "stored but never answered: 1" in report
