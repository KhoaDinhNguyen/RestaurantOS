# Contributing to RestaurantOS

This file covers how we work in this repository: setting up, testing, branching and reviewing. The design itself is in `docs/architecture.md`.

## 1. Set up your machine

Follow `docs/setup.md` for the system components (Git, Docker, uv, Node, PostgreSQL client). Ubuntu is the reference platform. Then:

```bash
git clone https://github.com/KhoaDinhNguyen/RestaurantOS.git
cd RestaurantOS

uv sync                                      # Python 3.14 + all Python deps into .venv
uv run pre-commit install --install-hooks    # git hooks (lint + commit message)

cd frontend && npm ci && cd ..               # once the frontend exists
```

Get the dataset from the team Drive folder and put the CSV files and `MANIFEST.csv` directly in `data/`. Check it against `MANIFEST.csv`. The `data/` folder is git-ignored. **Never commit or publish the dataset.**

## 2. Working agreement

- **`main` always works.** Once the initial setup (WP0) is in, work goes through branches and pull requests, not straight to `main`.
- **One branch per piece of work**, cut from an up-to-date `main`: `feat/gateway-geo-routing`, `fix/relay-duplicate-ack`, `docs/api`.
- **Open a pull request when it is ready to merge.** Keep PRs small: one component, one purpose. Open a draft PR early if you want feedback.
- **Anyone on the team may merge**, including their own PR, once CI is green. No approval is required. Ask for a review when you want one, and always for a contract change (see below).
- **Rebase onto the latest `main` before merging** (`git fetch && git rebase origin/main`), re-run the tests, then squash-merge and delete the branch.
- **Merges are done by a human.** Coding agents may open PRs but never merge them (see `AGENTS.md`).
- **Stay in your component's folder.** If you need a change in someone else's folder, ask them or open a separate PR and tag them.
- **Contracts are frozen.** The SQL schema (`db/`), the shared models (`src/restaurantos/common/models.py`), the HTTP routes and the Toxiproxy proxy names are what the other three people build against. To change one: open an issue, get agreement, and update `docs/architecture.md` in the same PR.
- **New dependencies need a heads-up** in the team chat first (backend). Frontend packages are up to the UI owners.

## 3. Before you commit

The hooks run on `git commit` and fix most things for you (formatting, imports, whitespace). If a hook changes files, `git add` them and commit again.

Run the tests for the components you touched:

```bash
uv run pytest tests/unit/gateway                        # one component
uv run pytest -m "not integration and not cluster"      # all unit tests (seconds)
```

## 4. Before you open a pull request

```bash
uv run pre-commit run --all-files
uv run pytest -m "not cluster"        # unit + integration; needs PostgreSQL on ROS_TEST_DSN
cd frontend && npm run lint && npm test -- --run
```

If you touched replication, failover, the relay or routing, also bring the cluster up and run the matching failure scenario (`uv run pytest -m cluster -k T1`). CI cannot run these.

Test levels:

| Marker | Needs | Runs in CI |
| --- | --- | --- |
| (none) | nothing | yes |
| `integration` | one PostgreSQL 18 | yes |
| `cluster` | full Compose cluster + Toxiproxy | no, run locally |

## 5. Commit messages

[Conventional Commits](https://www.conventionalcommits.org/): `type(scope): summary`.

- Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `ci`, `build`.
- Scope is the component: `gateway`, `region`, `relay`, `coordinator`, `sim`, `db`, `frontend`, `infra`.
- Example: `feat(relay): resume from last acknowledged outbox sequence`.

## 6. Pull request checklist

- [ ] What changed and why, in two or three sentences.
- [ ] How you tested it (commands, scenario IDs).
- [ ] New dependencies, if any.
- [ ] Contract changes, if any, with `docs/architecture.md` updated.
- [ ] Screenshots for UI changes.

## 7. Code conventions

- **Python**: 3.14, ruff for lint and format (line length 100), type hints on public functions, async I/O in services.
- **SQL**: hand-written, in `db/`, upper-case keywords, parameters as `%(name)s`. No ORM.
- **Time**: business logic uses simulated time passed in the request, never `now()`. Wall-clock timestamps are for measurements only.
- **Frontend**: React + Vite. No business logic in the browser.
- **Docs**: Markdown in `docs/`. Write "Section 2.3", not the section sign.

## 8. Where things live

See the repository layout and component ownership table in `docs/architecture.md`.
