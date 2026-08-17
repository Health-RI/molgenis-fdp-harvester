# SPDX-FileCopyrightText: 2024-present Health-RI
# SPDX-License-Identifier: AGPL-3.0-or-later

FROM python:3.12-alpine3.22
WORKDIR /app

ENV UV_PYTHON_DOWNLOADS=0
COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /uvx /bin/

COPY pyproject.toml uv.lock README.md ./
COPY src/ src/

RUN uv sync --locked --no-dev && \
    adduser -D -H -s /bin/false harvester

ENV PATH="/app/.venv/bin:$PATH"
USER harvester

ENTRYPOINT ["harvest"]
