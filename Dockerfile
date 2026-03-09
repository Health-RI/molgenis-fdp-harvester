# SPDX-FileCopyrightText: 2024-present Health-RI
# SPDX-License-Identifier: AGPL-3.0-or-later

FROM python:3.12-slim
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir . && \
    useradd --no-create-home --shell /bin/false harvester

USER harvester

ENTRYPOINT ["harvest"]
