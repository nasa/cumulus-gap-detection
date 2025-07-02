#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Variables
FUNCTION_NAME="gesdisc-cumulus-prod-gapConfig"
EVENT_FILE="event.json"
RESPONSE_FILE="response.json"

# Check if event.json exists
if [[ ! -f "$EVENT_FILE" ]]; then
  echo "Error: $EVENT_FILE not found."
  exit 1
fi

echo "Invoking Lambda function: $FUNCTION_NAME"
aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --payload file://"$EVENT_FILE" \
  "$RESPONSE_FILE" \
  --cli-read-timeout 0 \
  --cli-connect-timeout 0

echo "Invocation complete. Response saved to $RESPONSE_FILE"