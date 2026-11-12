# System setup

What to install on your machine before `uv sync`. **Ubuntu 24.04 LTS is the reference platform**: it is the only one we test and measure on. macOS and Windows notes are listed only where they differ.

PostgreSQL servers and Toxiproxy run **in Docker**. You do not install them on your machine.

| Component | Why | Version |
| --- | --- | --- |
| Git | source control | any recent |
| Docker Engine + Compose plugin | runs every PostgreSQL node, Toxiproxy and the services | Compose v2 |
| uv | Python and Python packages | 0.13 or newer |
| clang + libpq-dev | `psycopg[c]` compiles its C extension against libpq | n/a |
| Node.js + npm | frontend, eslint, prettier | the version in `frontend/.nvmrc` |
| PostgreSQL client (`psql`, `pg_dump`) | debugging and snapshots | 18 |
| Toxiproxy CLI (optional) | poking at proxies by hand | 2.12 |

Hardware: plan for about 8 GB of free RAM for the full cluster (eight PostgreSQL containers plus services) and 20 GB of disk.

## Ubuntu 24.04

```bash
# 1. Base tools, compiler and libpq headers
sudo apt-get update
sudo apt-get install -y git curl ca-certificates build-essential clang libpq-dev python3-dev

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

# 5. PostgreSQL 18 client tools (PGDG repository; Ubuntu's own package is older)
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
  https://www.postgresql.org/media/keys/ACCC4CF8.asc
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
https://apt.postgresql.org/pub/repos/apt $(. /etc/os-release && echo "$VERSION_CODENAME")-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update
sudo apt-get install -y postgresql-client-18
```

Optional Toxiproxy CLI: download `toxiproxy-cli` for your architecture from <https://github.com/Shopify/toxiproxy/releases> and put it on your `PATH`. Everything the CLI does is also available through the HTTP API on port 8474 and through `rosctl faults`.

Check:

```bash
docker run --rm hello-world && uv --version && node --version && psql --version
```

## macOS (differences only)

```bash
brew install git uv nvm libpq          # libpq provides the headers for building psycopg[c]
brew link --force libpq                 # puts psql, pg_dump and pg_config on PATH
brew install --cask docker              # Docker Desktop (OrbStack also works)
xcode-select --install                  # compiler for psycopg[c]
brew install toxiproxy                  # optional CLI
```

- Give Docker Desktop at least 8 GB of memory (Settings, Resources).
- Apple Silicon runs the arm64 PostgreSQL images. Functionally the same, but **do not report performance numbers from a Mac**.

## Windows (differences only)

Use **WSL 2 with Ubuntu 24.04** and follow the Ubuntu steps inside it. Native Windows is not supported: `psycopg[c]` needs the compiler and libpq headers from Ubuntu step 1, and our shell scripts assume bash.

1. `wsl --install -d Ubuntu-24.04` (PowerShell as administrator), then reboot.
2. Either install Docker Engine inside WSL (Ubuntu step 2), or install Docker Desktop and turn on WSL integration for the Ubuntu distro. Pick one, not both.
3. Clone the repository **inside the WSL file system** (`~/RestaurantOS`), not under `/mnt/c`. Bind mounts from `/mnt/c` are slow enough to distort every measurement.
4. Give WSL enough memory: in `%UserProfile%\.wslconfig` set `[wsl2]` and `memory=10GB`.

## After the system components

```bash
uv sync
uv run pre-commit install --install-hooks
cd frontend && npm ci
```

Continue with `CONTRIBUTING.md`.
