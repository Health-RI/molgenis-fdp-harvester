# SPDX-FileCopyrightText: 2024-present Health-RI
# SPDX-License-Identifier: AGPL-3.0-or-later

# ==========================================
# STAGE 1: Builder Phase
# ==========================================
FROM python:3.12-alpine3.22 AS builder

WORKDIR /app

# Optimize uv performance & force bytecode compilation
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=0

# Bring in uv binary from official image
COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /uvx /bin/

# Step A: Install dependencies FIRST (Layer Caching)
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Step B: Copy source code and install application
COPY src/ src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


# ==========================================
# STAGE 2: Runtime Production Phase
# ==========================================
FROM python:3.12-alpine3.22 AS runner

WORKDIR /app

# Build-time args for CI metadata
ARG VERSION="local"
ARG VCS_REF="dirty"
ARG BUILD_DATE="unknown"

# OCI-compliant labels
LABEL org.opencontainers.image.title="molgenis-fdp-harvester" \
      org.opencontainers.image.description="Harvests metadata and datasets from FAIR Data Points (FDP) and imports them into a MOLGENIS catalog." \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.url="https://www.health-ri.nl" \
      org.opencontainers.image.source="https://github.com/Health-RI/molgenis-fdp-harvester" \
      org.opencontainers.image.authors="Health-RI" \
      org.opencontainers.image.documentation="https://github.com/Health-RI/molgenis-fdp-harvester#readme"

# Backwards-compatible labels
LABEL name="molgenis-fdp-harvester" \
      description="Harvests metadata and datasets from FAIR Data Points (FDP) and imports them into a MOLGENIS catalog. Supports authenticated FDPs, configurable mappings, and scheduled or ad-hoc harvesting. Use x-molgenis-token to authenticate with MOLGENIS endpoint, must be supplied in config" \
      version="${VERSION}" \
      vcs-ref="${VCS_REF}" \
      releasenotes="https://github.com/Health-RI/molgenis-fdp-harvester/releases/tag/${VERSION}" \
      license="https://www.apache.org/licenses/LICENSE-2.0.txt" \
      authorization="This Dockerfile is intended to build a container image that will be publicly accessible in the platform images repository." \
      environment="production (Kubernetes/Knative)" \
      platform="kubernetes,knative" \
      vendor="Health-RI" \
      url="https://www.health-ri.nl"

# Create non-root user (Alpine native syntax) & runtime directories safely
RUN addgroup -g 1000 -S ds && \
    adduser -u 1000 -S -G ds -h /home/ds -s /sbin/nologin ds && \
    mkdir -p /home/ds/datasets /home/ds/persistent-home /home/ds/persistent-shared-folder && \
    chown -R ds:ds /home/ds /app

# Copy built virtual environment and application from stage 1
COPY --from=builder --chown=ds:ds /app /app

# Place virtual environment executables in PATH
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /home/ds
USER ds:ds

ENTRYPOINT ["harvest"]