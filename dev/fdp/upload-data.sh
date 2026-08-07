#!/bin/sh

set -eu

# Usage:
#   BEARER_TOKEN="<jwt>" ./dev/fdp/upload-data.sh
# Optional:
#   BASE_URL="http://localhost:8081" (default shown)

BASE_URL="${BASE_URL:-http://localhost:8081}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
BEARER_TOKEN="${BEARER_TOKEN:-}"

if [ -z "$BEARER_TOKEN" ]; then
    echo "Error: BEARER_TOKEN must be set in the environment." >&2
    echo "Example: BEARER_TOKEN=... ./dev/fdp/upload-data.sh" >&2
    exit 1
fi

post_turtle_and_get_uuid() {
    ENDPOINT="$1"
    PAYLOAD="$2"

    HEADERS_FILE="$(mktemp)"
    BODY_FILE="$(mktemp)"

    CODE="$(curl -sS -o "$BODY_FILE" -D "$HEADERS_FILE" -w '%{http_code}' \
        -X POST "$BASE_URL/$ENDPOINT" \
        -H "Authorization: Bearer $BEARER_TOKEN" \
        -H 'Content-Type: text/turtle' \
        -H 'Accept: text/turtle' \
        --data-raw "$PAYLOAD")"

    if [ "$CODE" -lt 200 ] || [ "$CODE" -ge 300 ]; then
        echo "Error: POST /$ENDPOINT failed with HTTP $CODE" >&2
        cat "$BODY_FILE" >&2
        rm -f "$HEADERS_FILE" "$BODY_FILE"
        exit 1
    fi

    LOCATION="$(awk 'BEGIN { IGNORECASE=1 } /^Location:/{ print $2 }' "$HEADERS_FILE" | tr -d '\r' | tail -n 1)"
    UUID="$(printf '%s\n' "$LOCATION" | sed -nE 's#.*/([0-9a-fA-F-]{36})$#\1#p')"

    if [ -z "$UUID" ]; then
        # Fallback if no Location header is present.
        UUID="$(grep -Eo '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' "$BODY_FILE" | head -n 1 || true)"
    fi

    if [ -z "$UUID" ]; then
        echo "Error: Could not determine UUID for /$ENDPOINT" >&2
        cat "$BODY_FILE" >&2
        rm -f "$HEADERS_FILE" "$BODY_FILE"
        exit 1
    fi

    rm -f "$HEADERS_FILE" "$BODY_FILE"
    printf '%s\n' "$UUID"
}

publish_resource() {
    RESOURCE="$1"
    UUID="$2"

    curl -fsS -X PUT "$BASE_URL/$RESOURCE/$UUID/meta/state" \
        -H "Authorization: Bearer $BEARER_TOKEN" \
        -H 'Content-Type: application/json' \
        -H 'Accept: application/json' \
        --data-raw '{"current":"PUBLISHED"}' > /dev/null
}

CATALOG_TTL_FILE="$SCRIPT_DIR/data/test-catalog.ttl"
if [ ! -f "$CATALOG_TTL_FILE" ]; then
    echo "Error: Catalog template not found at $CATALOG_TTL_FILE" >&2
    exit 1
fi

CATALOG_PAYLOAD="$(cat "$CATALOG_TTL_FILE")"

CATALOG_UUID="$(post_turtle_and_get_uuid 'catalog' "$CATALOG_PAYLOAD")"
publish_resource 'catalog' "$CATALOG_UUID"
echo "Created and published catalog: $CATALOG_UUID"

DATASET_TTL_FILE="$SCRIPT_DIR/data/test-data.ttl"
if [ ! -f "$DATASET_TTL_FILE" ]; then
    echo "Error: Dataset template not found at $DATASET_TTL_FILE" >&2
    exit 1
fi

DATASET_PAYLOAD="$(sed "s|cat:__CATALOG_UUID__|cat:$CATALOG_UUID|" "$DATASET_TTL_FILE")"

DATASET_UUID="$(post_turtle_and_get_uuid 'dataset' "$DATASET_PAYLOAD")"
publish_resource 'dataset' "$DATASET_UUID"
echo "Created and published dataset: $DATASET_UUID"