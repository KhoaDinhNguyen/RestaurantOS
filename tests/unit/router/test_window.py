from datetime import UTC, time

import pytest

from restaurantos.gateway.router.window import normalize_window


def test_day_becomes_one_day_range() -> None:
    window = normalize_window(
        day=14,
        from_day=None,
        to_day=None,
        from_time=None,
        to_time=None,
    )

    assert window.from_day == 14
    assert window.to_day == 14
    assert not window.has_time_window
    assert not window.crosses_midnight


def test_from_day_without_to_day_is_one_day() -> None:
    window = normalize_window(
        day=None,
        from_day=14,
        to_day=None,
        from_time=None,
        to_time=None,
    )

    assert (window.from_day, window.to_day) == (14, 14)


def test_day_range() -> None:
    window = normalize_window(
        day=None,
        from_day=14,
        to_day=20,
        from_time=None,
        to_time=None,
    )

    assert (window.from_day, window.to_day) == (14, 20)


def test_normal_time_window() -> None:
    window = normalize_window(
        day=14,
        from_day=None,
        to_day=None,
        from_time=time(11, 0),
        to_time=time(14, 0),
    )

    assert window.has_time_window
    assert not window.crosses_midnight


def test_cross_midnight_window() -> None:
    window = normalize_window(
        day=20,
        from_day=None,
        to_day=None,
        from_time=time(22, 0),
        to_time=time(4, 0),
    )

    assert window.has_time_window
    assert window.crosses_midnight


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "day": 14,
            "from_day": 14,
            "to_day": None,
            "from_time": None,
            "to_time": None,
        },
        {
            "day": None,
            "from_day": None,
            "to_day": None,
            "from_time": None,
            "to_time": None,
        },
        {
            "day": None,
            "from_day": 20,
            "to_day": 19,
            "from_time": None,
            "to_time": None,
        },
        {
            "day": 14,
            "from_day": None,
            "to_day": None,
            "from_time": time(12, 0),
            "to_time": None,
        },
    ],
)
def test_invalid_windows(kwargs) -> None:
    with pytest.raises(ValueError):
        normalize_window(**kwargs)


def test_rejects_time_with_utc_offset() -> None:
    with pytest.raises(
        ValueError,
        match="must not include a UTC offset",
    ):
        normalize_window(
            day=14,
            from_day=None,
            to_day=None,
            from_time=time(9, 0, tzinfo=UTC),
            to_time=time(17, 0, tzinfo=UTC),
        )
