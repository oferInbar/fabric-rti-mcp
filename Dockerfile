# syntax=docker/dockerfile:1.7

# ---------- Builder ----------
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY defender_ah_mcp ./defender_ah_mcp

ARG PACKAGE_VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DEFENDER_ADVANCED_HUNTING_MCP=${PACKAGE_VERSION}

RUN pip install --upgrade pip build \
    && pip wheel --no-deps --wheel-dir /wheels . \
    && pip wheel --wheel-dir /wheels \
        "httpx~=0.28.1" \
        "fastmcp>=3.0,<4" \
        "azure-identity>=1.24.0,<2" \
        "msal>=1.32.0,<2"

# ---------- Runtime ----------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEFENDER_AH_TRANSPORT=http \
    DEFENDER_AH_HTTP_HOST=0.0.0.0 \
    DEFENDER_AH_HTTP_PORT=3000

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-index --find-links=/wheels defender-advanced-hunting-mcp \
    && pip install debugpy \
    && rm -rf /wheels

# Install Azure CLI so AzureCliCredential works inside the container
# (when host ~/.azure is mounted at /home/mcp/.azure).
RUN pip install azure-cli

RUN useradd --create-home --uid 1000 mcp
USER mcp

EXPOSE 3000

ENTRYPOINT ["defender-advanced-hunting-mcp"]
