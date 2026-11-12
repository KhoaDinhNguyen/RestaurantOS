from datetime import time

import pytest

from restaurantos.common.simclock import SimClock, from_sim_seconds, to_sim_seconds


def test_sim_seconds_round_trip():
    s = to_sim_seconds(85, time(12, 3, 29))
    assert s == 85 * 86400 + 12 * 3600 + 3 * 60 + 29
    assert from_sim_seconds(s) == (85, time(12, 3, 29))


class FakeWall:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


def test_clock_does_not_move_until_started():
    wall = FakeWall()
    clock = SimClock(start_sim_seconds=1000, speed=10, wall=wall)
    wall.t += 5
    assert clock.now() == 1000


def test_speed_change_does_not_jump():
    wall = FakeWall()
    clock = SimClock(start_sim_seconds=0, speed=10, wall=wall)
    clock.start()
    wall.t += 2  # 20 simulated seconds
    clock.set_speed(1000)
    assert clock.now() == pytest.approx(20)
    wall.t += 1  # 1000 more
    assert clock.now() == pytest.approx(1020)


def test_pause_and_resume():
    wall = FakeWall()
    clock = SimClock(speed=100, wall=wall)
    clock.start()
    wall.t += 1
    clock.pause()
    wall.t += 50
    assert clock.now() == pytest.approx(100)
    clock.start()
    wall.t += 1
    assert clock.now() == pytest.approx(200)


def test_wall_seconds_until():
    wall = FakeWall()
    clock = SimClock(speed=1000, wall=wall)
    clock.start()
    assert clock.wall_seconds_until(86400) == pytest.approx(86.4)
    assert clock.wall_seconds_until(-5) == 0


def test_bad_speed_rejected():
    with pytest.raises(ValueError):
        SimClock(speed=0)
