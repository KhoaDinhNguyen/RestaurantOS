# AGENTS.md

Instructions for coding agents (Claude Code, Codex, Cursor, and others) working in this repository. Human contributors: see `CONTRIBUTING.md`.

## Read first

1. `docs/architecture.md`: components, contracts, schema, and who owns which folder.
2. `CONTRIBUTING.md`: setup, tests, branch and commit rules.

## Hard rules

- **Never merge a pull request.** Do not run `gh pr merge`, do not push to `main`, do not enable auto-merge. Open the PR and stop. A human merges.
- **Never add, remove or upgrade a dependency without asking the user first.** This covers `pyproject.toml`, `uv.lock`, Docker images and system packages. Say which package, why, and what the alternative without it would be. The backend should rarely need anything new.
  - Exception: inside `frontend/` you may add npm packages when the task needs them. List them in the PR description.
- **Never commit dataset files.** Nothing under `data/`, `snapshots/` or `runs/` goes into git. The DHRD files must not be published.
- **Do not change a contract on your own.** The contracts are: the SQL schema in `db/`, the Pydantic models in `src/restaurantos/common/models.py`, the HTTP routes listed in `docs/architecture.md`, and the Toxiproxy proxy names. If a task seems to need a contract change, stop and ask.
- **Stay inside the folder(s) the task names.** If a fix needs a change in someone else's component, describe it in the PR instead of making it.
- Do not bypass hooks (`--no-verify`) and do not weaken or delete a failing test to make it pass.

## Project conventions

- Python 3.14, managed by `uv`. Run everything through `uv run ...`. Never call `pip` directly.
- SQL is hand-written and lives in `db/`. No ORM, no query builder. Python loads named `.sql` files and passes parameters with psycopg (`%(name)s`).
- Services are async (FastAPI + psycopg 3 async pools).
- Business time is **simulated time** carried in the request (`order_day`, `order_time`). Never use `now()` or `datetime.now()` to decide what "today" is. Wall-clock time is only for measurements (`received_at`, `committed_at`, `acked_at`, `hq_applied_at`).
- Every write to a region carries the region epoch. Every database connection that a failure test can cut goes through its Toxiproxy proxy, never straight to the container.
- The frontend does no business logic: no aggregation, no joins, no routing. It calls the HTTP API and renders the JSON.
- Docs: write "Section 2.3", never the section sign. The project stages are "Phase 1/2/3". Do not cite Wikipedia.

## Commands

```bash
uv sync                               # install Python deps (incl. dev)
uv run pre-commit run --all-files     # lint + format everything
uv run pytest -m "not integration and not cluster"   # fast unit tests
uv run pytest -m "not cluster"        # + integration (needs PostgreSQL)
cd frontend && npm ci && npm test     # frontend
```

## Before you say a task is done

- Tests for every component you touched pass, and you ran them.
- `uv run pre-commit run --all-files` is clean.
- The PR description says what changed, how you tested it, and lists any new dependency or contract question.
- Commit messages follow Conventional Commits: `feat(gateway): ...`, `fix(relay): ...`.
