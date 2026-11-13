from restaurantos.common.config import Settings


def settings(**values):
    # _env_file=None: a developer's own .env must not change what these tests see.
    return Settings(_env_file=None, **values)


def test_toxiproxy_url_follows_the_network(monkeypatch):
    monkeypatch.delenv("ROS_NETWORK", raising=False)
    monkeypatch.delenv("ROS_TOXIPROXY_URL", raising=False)
    assert settings().toxiproxy_url == "http://toxiproxy:8474"
    assert settings(network="host").toxiproxy_url == "http://127.0.0.1:8474"


def test_an_explicit_toxiproxy_url_wins(monkeypatch):
    monkeypatch.delenv("ROS_NETWORK", raising=False)
    assert settings(network="host", toxiproxy_url="http://x:1").toxiproxy_url == "http://x:1"
    monkeypatch.setenv("ROS_NETWORK", "host")
    monkeypatch.setenv("ROS_TOXIPROXY_URL", "http://y:2")
    assert settings().toxiproxy_url == "http://y:2"


def test_the_deadline_is_longer_than_the_statement_timeout(monkeypatch):
    for name in ("ROS_REQUEST_DEADLINE_S", "ROS_STATEMENT_TIMEOUT_MS"):
        monkeypatch.delenv(name, raising=False)
    s = settings()
    # The server should get the chance to report a slow statement first.
    assert s.request_deadline_s * 1000 > s.statement_timeout_ms
