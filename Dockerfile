# syntax=docker/dockerfile:1
#
# Builds the `nasr` CLI image. Designed to work identically with both the
# Docker engine and Apple's `container` CLI by sticking to vanilla OCI features
# (no BuildKit-only mounts, no platform-specific RUN options).

FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:/root/.local/bin:$PATH

# System packages:
#   libsqlite3-mod-spatialite -> SpatiaLite extension loaded by geometry.py
#   ca-certificates           -> required for httpx -> external-api.faa.gov
# pyogrio's wheels bundle their own GDAL, so we don't install gdal-bin/libgdal-dev.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        libsqlite3-mod-spatialite \
 && rm -rf /var/lib/apt/lists/*

# Install uv from its official image (works in any OCI builder, no curl required).
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /usr/local/bin/uv

WORKDIR /app

# Lockfile-first install for layer caching.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev

VOLUME ["/data"]
WORKDIR /data

ENTRYPOINT ["nasr"]
CMD ["--help"]
