"""The hash-based randomness rule: every value a pure function of
(seed, order_id, purpose). No shared generator, no dependence on send
order."""

from __future__ import annotations

from restaurantos.sim.determinism import stable_digest, stable_float, stable_int


def test_same_inputs_give_the_same_value() -> None:
    for _ in range(5):
        assert stable_float(42, "17", "jitter") == stable_float(42, "17", "jitter")


def test_every_input_matters() -> None:
    base = stable_float(42, "17", "jitter")
    assert stable_float(43, "17", "jitter") != base
    assert stable_float(42, "18", "jitter") != base
    assert stable_float(42, "17", "resend") != base


def test_values_stay_in_range() -> None:
    for i in range(50):
        value = stable_float(7, str(i), "unit")
        assert 0.0 <= value < 1.0
        assert stable_int(7, str(i), "unit", 5) in range(5)


def test_high_must_be_positive() -> None:
    try:
        stable_int(7, "17", "unit", 0)
    except ValueError as error:
        assert "positive" in str(error)
    else:
        raise AssertionError("stable_int accepted high=0")


def test_digest_is_sha256_length() -> None:
    assert len(stable_digest(0, "o", "p")) == 32
