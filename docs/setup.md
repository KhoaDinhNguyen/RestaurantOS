# System setup

What to install on your machine before `uv sync`. **Ubuntu 24.04 LTS is the reference platform**: it is the only one we test and measure on. macOS and Windows notes are listed only where they differ.

PostgreSQL servers and Toxiproxy run **in Docker**. You do not install them on your machine.

| Component | Why | Version |
| --- | --- | --- |
| Git | source control | any recent |
| Docker Engine + Compose plugin | runs every PostgreSQL node, Toxiproxy and the services | Compose v2 |
| uv | Python and Python packages | 0.13 or newer |
| clang + libpq-dev | `psycopg[c]` compiles its C extension against libpq | **libpq 17 or newer** (from the PGDG repository; Ubuntu 24.04's own is 16) |
| Node.js + npm | frontend, eslint, prettier | the version in `frontend/.nvmrc` |
| PostgreSQL client (`psql`, `pg_dump`) | debugging and snapshots | 18 |
| Toxiproxy CLI (optional) | poking at proxies by hand | 2.12 |

Hardware: plan for about 8 GB of free RAM for the full cluster (eight PostgreSQL containers plus services) and 20 GB of disk.

The 20 GB covers short runs over a window of days. Three things grow past it on long runs, so check `docker system df -v` after your first full run and correct this number:

- **Outbox rows are never deleted**, and every region's data exists again on its standby and in HQ's analytics copy.
- **WAL held for a standby that is cut off.** Each standby has a replication slot, so its primary keeps WAL while the link is down, up to `max_slot_wal_keep_size` = 2 GB per primary (set in the Compose file). Disk use from this is bounded, but a standby left cut for longer than 2 GB of WAL cannot catch up any more and has to be cloned again. How soon that happens depends on the playback speed; measure it before you plan a long cut.
- **Each snapshot is a full copy of every data volume.**

## Ubuntu 24.04

```bash
# 1. Base tools and compiler
sudo apt-get update
sudo apt-get install -y git curl ca-certificates build-essential clang python3-dev

# 2. Docker Engine + Compose plugin (official Docker repository)
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker "$USER"      # log out and back in afterwards
docker compose version

# 3. uv (installs and manages Python 3.14 for the project)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 4. Node.js through nvm (use the version the frontend pins)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash
# open a new terminal, then, inside the repository:
nvm install     # reads frontend/.nvmrc; until that file exists: nvm install --lts

# 5. libpq headers and PostgreSQL 18 client tools, both from the PGDG repository.
#    Ubuntu's own packages are version 16, and psycopg must be built against
#    libpq 17 or newer (see "libpq 17 or newer" below). Do this before `uv sync`.
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
  https://www.postgresql.org/media/keys/ACCC4CF8.asc
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
https://apt.postgresql.org/pub/repos/apt $(. /etc/os-release && echo "$VERSION_CODENAME")-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update
sudo apt-get install -y libpq-dev postgresql-client-18
pg_config --version                  # must say 17 or newer
```

Optional Toxiproxy CLI: download `toxiproxy-cli` for your architecture from <https://github.com/Shopify/toxiproxy/releases> and put it on your `PATH`. Everything the CLI does is also available through the HTTP API on port 8474 and through `rosctl faults`.

Check:

```bash
docker run --rm hello-world && uv --version && node --version && psql --version && pg_config --version
```

### libpq 17 or newer

`psycopg[c]` is compiled on your machine against whatever libpq headers it finds. Below version 17, psycopg cancels a running query with a blocking call. On a link that a fault test has silenced, that call freezes the whole service for about a minute, every region included. From 17 on, the cancel does not block. The unit test `tests/unit/common/test_environment.py` fails with a pointer to this section when the build is too old.

If you set your machine up before this rule (psycopg built against libpq 16), fix it once:

```bash
# add the PGDG repository as in step 5 above, then:
sudo apt-get install -y libpq-dev postgresql-client-18
uv cache clean psycopg-c
uv sync --reinstall-package psycopg-c
uv run python -c "import psycopg; print(psycopg.pq.__build_version__, psycopg.capabilities.has_cancel_safe())"
# expect 170000 or higher, and True
```

## macOS (differences only)

```bash
brew install git uv nvm libpq          # headers for building psycopg[c]; Homebrew's libpq is 17 or newer
brew link --force libpq                 # puts psql, pg_dump and pg_config on PATH
brew install --cask docker              # Docker Desktop (OrbStack also works)
xcode-select --install                  # compiler for psycopg[c]
brew install toxiproxy                  # optional CLI
```

- Give Docker Desktop at least 8 GB of memory (Settings, Resources).
- Apple Silicon runs the arm64 PostgreSQL images. Functionally the same, but **do not report performance numbers from a Mac**.

## Windows (differences only)

Use **WSL 2 with Ubuntu 24.04** and follow the Ubuntu steps inside it. Native Windows is not supported: `psycopg[c]` needs the compiler from Ubuntu step 1 and the libpq headers from step 5, and our shell scripts assume bash.

1. `wsl --install -d Ubuntu-24.04` (PowerShell as administrator), then reboot.
2. Either install Docker Engine inside WSL (Ubuntu step 2), or install Docker Desktop and turn on WSL integration for the Ubuntu distro. Pick one, not both.
3. Clone the repository **inside the WSL file system** (`~/RestaurantOS`), not under `/mnt/c`. Bind mounts from `/mnt/c` are slow enough to distort every measurement.
4. Give WSL enough memory: in `%UserProfile%\.wslconfig` set `[wsl2]` and `memory=10GB`.

## Dataset

The cleaned DHRD dataset lives in the course Google Drive folder (ASU accounts only): <https://drive.google.com/drive/folders/1l8tAMZtShxXXdluNqXpHsuTBVB1W-GZ0?usp=sharing>. Download the `*.csv` files and `MANIFEST.csv` into `data/` at the repository root. That directory is git-ignored and the dataset files must never be committed or published.

Services and `rosctl` read the dataset from the path in `ROS_DATA_DIR`, which defaults to `data/`.

CI and the test suite never use the real dataset. Tests run on the hand-made mini dataset in `tests/fixtures/mini/`.

## After the system components

```bash
uv sync
uv run pre-commit install --install-hooks
cd frontend && npm ci
```

Continue with `CONTRIBUTING.md`.

## Running services on your machine (host mode)

While you develop, the databases and Toxiproxy run in Docker and your Python service runs directly on your machine, so there is no image to rebuild after an edit and a debugger can attach. Nothing extra needs installing.

```bash
docker compose -f infra/compose.yaml up -d      # HQ, one region, Toxiproxy
cp .env.example .env                            # then set ROS_NETWORK=host in it
uv run pytest -m cluster tests/cluster/test_deadline.py    # checks the path end to end
```

- With `ROS_NETWORK=host`, a service reaches every database on `127.0.0.1`, through the same Toxiproxy proxies a container would use. The Compose file publishes those ports on `127.0.0.1` only, so they are not visible from the network your machine is on.
- These ports must be free for the dev baseline: 8474; 5500, 5501, 5510 and 5511; 21000 to 21005 and 21010 to 21019. Each extra region adds ten proxy ports and two database ports.
- The ports `rosctl up` needs free: gateway 8000, coordinator 8001, and the edge API of region n on 8010 + n (so 8011 up to 8010 + N for an N-region cluster). Port 8002 is not one of them - it is the default of the playback control routes (`ROS_SIM_CONTROL_PORT`, or `--port` on `rosctl playback start`), served while a playback run is live.
- `rosctl up` and `rosctl down` need Linux or WSL2: their process checks read `/proc`. On macOS, start each service by hand with the commands in the architecture Section 4 table.
- Host mode is for developing and debugging. Do not report performance numbers from it: every query takes an extra hop through Docker's port forwarding. Measured runs use container mode (`docs/architecture.md` Section 4).

## The service image (container mode)

One image runs every Python service (gateway, edge, coordinator, relay, HQ worker); which one it is comes from environment variables at `docker run` time, never from the image.

```bash
# from the repository root
docker build -f infra/Dockerfile -t restaurantos-services:local .
```

Inside the image: the virtualenv at `/app/.venv` (on `PATH`), `src/` and `db/`; nothing else. No `.env`, no dataset: settings come only from the environment (`--env-file`), and the dataset never enters an image (`.dockerignore` is an allowlist).

The image uses Debian trixie's own `libpq5` 17, while the host builds `psycopg[c]` against PGDG's libpq (see "libpq 17 or newer" above). Both meet the 17-or-newer rule: below 17, psycopg cancels a query with a blocking `PQcancel` that freezes a whole process on a link a fault test has silenced.

The cluster definition is **mounted, not copied**: a region-count change needs no rebuild.

A service container joins the cluster's Compose network (named after the Compose project; `restaurantos_default` for the default project name) and publishes its HTTP port on the loopback interface only:

```bash
docker run --rm \
  --network restaurantos_default \
  --env-file .env.example \
  -e ROS_NETWORK=compose \
  -e ROS_CLUSTER_FILE=/app/infra/cluster.toml \
  -v "$PWD/infra/cluster.toml:/app/infra/cluster.toml:ro" \
  -p 127.0.0.1:8000:8000 \
  restaurantos-services:local python -m restaurantos.gateway
```

### Starting the whole system in container mode

`rosctl bootstrap` writes the service entries into `infra/generated/compose.yaml` too - one per service, the same names, ports and identity environment as host mode, with `ROS_NETWORK=compose` - behind the `services` profile. It also copies the cluster config it ran with to `infra/generated/cluster.toml`, which every service entry mounts, and starts no service containers itself: bootstrap's own `up -d --wait` is profile-gated to the databases and Toxiproxy, so a data load never starts them.

```bash
# start every service container (after `uv run rosctl bootstrap`)
docker compose -p restaurantos -f infra/generated/compose.yaml \
  --profile services up -d --wait

# stop the service containers WITHOUT touching the databases (one region;
# an N-region cluster also has relay-r<n> and edge-r<n> for every region)
docker compose -p restaurantos -f infra/generated/compose.yaml \
  --profile services stop gateway coordinator hq-worker relay-r1 edge-r1
docker compose -p restaurantos -f infra/generated/compose.yaml \
  --profile services rm -f gateway coordinator hq-worker relay-r1 edge-r1
```

`docker compose ps` then shows each service as healthy (the entry's healthcheck is a stdlib `urllib.request` call against its readiness route - the image has no curl). Never a bare `docker compose down` to stop the services: it removes the database containers as well. A fresh `rosctl bootstrap` tears the service containers down with the rest of the cluster.

Host and container mode use the same loopback ports (`8000`, `8001`, `8010 + n`), so never run both at once: a host-mode `rosctl up` and the container-mode command above fight over the same ports. `rosctl up` and `rosctl down` stay host-mode; they never manage containers.
