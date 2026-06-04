# SPDX-FileCopyrightText: 2024-present Health-RI
# SPDX-License-Identifier: AGPL-3.0-or-later

FROM python:3.12-alpine3.22
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir . && \
    adduser -D -H -s /bin/false harvester

USER harvester

ENTRYPOINT ["harvest"]
