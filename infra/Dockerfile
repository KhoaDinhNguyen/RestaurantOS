# The one Python service image (docs/architecture.md D16). Every Python
# service runs from it: gateway and edge (same code, different mode),
# coordinator, relay (region and HQ roles). Who the process is comes from
# environment variables at `docker run` time, never from the image.
# Build: docker build -f infra/Dockerfile -t restaurantos-services:local .

# Builder: compile psycopg-c against Debian trixie's own libpq 17 headers.
# No PGDG repository: python:3.14-slim-trixie IS Debian 13 (trixie), which
# ships PostgreSQL 17 (docs/setup.md, "17 or newer").
FROM python:3.14-slim-trixie AS builder

ENV UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

COPY --from=ghcr.io/astral-sh/uv:0.10.10 /uv /usr/local/bin/uv

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq-dev gcc libc6-dev \
    && apt-cache policy libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies first, for layer caching. --no-install-project keeps this
# layer independent of the source tree.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --no-dev --no-install-project

# Then the project itself. The default editable install keeps /app/src and
# /app/db mirroring the repository layout, which common.sqlfiles.db_dir()
# relies on (Path(__file__).parents[3] / "db").
COPY src/ src/
COPY db/ db/
RUN uv sync --locked --no-dev

# Final: runtime only. No toolchain, no -dev packages, no docs, no tests.
# Same base tag as the builder: /app/.venv/bin/python symlinks into this
# image's interpreter.
FROM python:3.14-slim-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && apt-cache policy libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/db /app/db
# Read at runtime by coordinator.rejoin (ROOT = parents[3] = /app) and
# mounted into the recovery Compose run from there.
COPY infra/postgres/rejoin-node.sh /app/infra/postgres/rejoin-node.sh

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Non-root, fixed UID/GID. /app is root-owned and read-only for this user;
# nothing in it needs to be writable.
RUN groupadd --gid 10001 ros \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin ros
USER 10001:10001

# No ENTRYPOINT: the Compose file (WP6a) passes the service command. The
# default just proves the image imports; restaurantos has no __version__.
CMD ["python", "-c", "import importlib.metadata as m; print('restaurantos', m.version('restaurantos'))"]
