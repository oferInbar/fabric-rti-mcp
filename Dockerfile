# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY defender_ah_mcp ./defender_ah_mcp

# setuptools_scm needs git history to derive a version; fall back to a
# pretend version since we don't ship .git into the image.
ARG VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION}

RUN pip install --upgrade pip && pip install .

# Run as non-root
RUN useradd --create-home --uid 1001 mcp && chown -R mcp:mcp /app
USER mcp

# Default to HTTP transport for container deployments.
ENV DEFENDER_AH_TRANSPORT=http \
    DEFENDER_AH_HTTP_HOST=0.0.0.0 \
    DEFENDER_AH_HTTP_PORT=3000 \
    DEFENDER_AH_HTTP_PATH=/mcp

EXPOSE 3000

ENTRYPOINT ["defender-advanced-hunting-mcp"]
