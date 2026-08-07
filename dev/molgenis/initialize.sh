#!/usr/bin/bash

set -euo pipefail

GRAPHQL_URL="${GRAPHQL_URL:-http://localhost:8080/api/graphql}"
USERNAME="${GRAPHQL_USERNAME:-admin}"
PASSWORD="${GRAPHQL_PASSWORD:-admin}"

RESPONSE=$(curl -s -i -X POST "$GRAPHQL_URL" \
  --data-raw "{\"query\":\"mutation{signin(email:\\\"$USERNAME\\\", password:\\\"$PASSWORD\\\"){status,message}}\"}")

if printf '%s' "$RESPONSE" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"SUCCESS"'; then
    echo "Login succeeded."
else
    echo "Login failed." >&2
    printf '%s\n' "$RESPONSE" >&2
    exit 1
fi
COOKIE=$(printf '%s\n' "$RESPONSE" | awk -F': ' '/^Set-Cookie:/ {print $2}' | head -n1 | cut -d';' -f1)

# Create database
CREATE_SCHEMA_RESPONSE=$(curl -sS -X 'POST' "$GRAPHQL_URL" \
    -H "Cookie: $COOKIE" \
    --data-raw '{"query":"mutation createSchema($name:String, $description:String, $template: String, $includeDemoData: Boolean){createSchema(name:$name, description:$description, template: $template, includeDemoData: $includeDemoData){message, taskId}}","variables":{"name":"Eucaim","description":null,"template":null,"includeDemoData":false},"operationName":"createSchema"}')

SCHEMA_NAME="Eucaim"
if printf '%s' "$CREATE_SCHEMA_RESPONSE" | grep -Ei 'already exists'; then
    echo "Schema '$SCHEMA_NAME' already exists; continuing."
elif printf '%s' "$CREATE_SCHEMA_RESPONSE" | grep -Fq '"errors"'; then
    echo "Failed to create schema:" >&2
    printf '%s\n' "$CREATE_SCHEMA_RESPONSE" >&2
    exit 1
else
    echo "Schema '$SCHEMA_NAME' created successfully."
fi

# Create token
TOKEN_RESPONSE=$(curl -sS -X 'POST' "$GRAPHQL_URL" \
    -H "Cookie: $COOKIE" \
    --data-raw '{"query":"mutation createToken($email:String,$tokenName:String){createToken(email:$email,tokenName:$tokenName){message,token}}","variables":{"email":"admin","tokenName":"test"}}')

if printf '%s' "$TOKEN_RESPONSE" | grep -Fq '"errors"'; then
    echo "Failed to create token:" >&2
    printf '%s\n' "$TOKEN_RESPONSE" >&2
    exit 1
fi

echo "Token created successfully."
TOKEN_VALUE=$(printf '%s' "$TOKEN_RESPONSE" | grep -o '"token"[[:space:]]*:[[:space:]]*"[^"]*"' | head -n1 | sed -E 's/.*"token"[[:space:]]*:[[:space:]]*"([^"]*)"/\1/')

if [[ -n "$TOKEN_VALUE" ]]; then
    echo
    echo "MOLGENIS_TOKEN=$TOKEN_VALUE"
    echo
else
    printf '%s\n' "$TOKEN_RESPONSE"
fi

echo "NOTE: Please upload the Molgenis metadata model in the browser"
echo
echo "Initialization completed successfully."
