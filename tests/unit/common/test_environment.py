"""The machine is set up the way the code relies on."""

import psycopg


def test_psycopg_is_built_against_libpq_17_or_newer():
    # Below libpq 17, psycopg cancels a query with the blocking PQcancel. On a
    # link that a fault test has silenced, that call blocks the whole process
    # for about a minute. From 17 on, the cancel is non-blocking and bounded.
    built = psycopg.pq.version_pretty(psycopg.pq.__build_version__)
    loaded = psycopg.pq.version_pretty(psycopg.pq.version())
    assert psycopg.capabilities.has_cancel_safe(), (
        f"psycopg[{psycopg.pq.__impl__}] was built against libpq {built} and loads libpq"
        f" {loaded}; RestaurantOS needs 17 or newer for both."
        " Install libpq-dev from the PGDG repository and rebuild psycopg:"
        " see 'libpq 17 or newer' in docs/setup.md."
    )
