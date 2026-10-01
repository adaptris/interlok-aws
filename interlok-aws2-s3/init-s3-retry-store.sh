#!/bin/bash

# Set up AWS CLI to use LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=eu-west-2

# Create a bucket (if it doesn't exist)
aws s3 mb s3://retry-store --endpoint-url http://localhost:4566
LARGE_BUCKET="retry-store-over-1000"
aws s3 mb "s3://${LARGE_BUCKET}" --endpoint-url http://localhost:4566

# Create example failed message 1
MSG_ID_1="msg-001-abc123"
aws s3 cp - s3://retry-store/${MSG_ID_1}/payload.blob \
  --endpoint-url http://localhost:4566 \
  <<< "This is test payload content"

echo "test.metadata=value1" | \
aws s3 cp - s3://retry-store/${MSG_ID_1}/metadata.properties \
  --endpoint-url http://localhost:4566

echo "java.lang.RuntimeException: Test error occurred" | \
aws s3 cp - s3://retry-store/${MSG_ID_1}/stacktrace.txt \
  --endpoint-url http://localhost:4566

# Create example failed message 2
MSG_ID_2="msg-002-def456"
aws s3 cp - s3://retry-store/${MSG_ID_2}/payload.blob \
  --endpoint-url http://localhost:4566 \
  <<< "Another test payload"

aws s3 cp - s3://retry-store/${MSG_ID_2}/metadata.properties \
  --endpoint-url http://localhost:4566 \
  <<< "key=value"

aws s3 cp - s3://retry-store/${MSG_ID_2}/stacktrace.txt \
  --endpoint-url http://localhost:4566 \
  <<< "java.io.IOException: Connection failed"

# Create a separate bucket with more than 1000 messages for listing-limit tests
if ! LARGE_MESSAGES_DIR=$(mktemp -d); then
  echo "Failed to create temporary directory for large retry-store test data" >&2
  exit 1
fi
trap 'rm -rf -- "$LARGE_MESSAGES_DIR"' EXIT

for ((i = 1; i <= 1005; i++)); do
  MSG_ID=$(printf 'msg-large-%04d' "$i")
  MSG_DIR="${LARGE_MESSAGES_DIR}/${MSG_ID}"
  mkdir -p "$MSG_DIR"
  printf 'Test payload for %s\n' "$MSG_ID" > "${MSG_DIR}/payload.blob"
  printf 'test.messageId=%s\n' "$MSG_ID" > "${MSG_DIR}/metadata.properties"
  printf 'java.lang.RuntimeException: Test error for %s\n' "$MSG_ID" > "${MSG_DIR}/stacktrace.txt"
done

aws s3 sync "$LARGE_MESSAGES_DIR" "s3://${LARGE_BUCKET}" \
  --endpoint-url http://localhost:4566

# Verify the objects were created
aws s3 ls s3://retry-store --recursive --endpoint-url http://localhost:4566
aws s3 ls "s3://${LARGE_BUCKET}" --recursive --endpoint-url http://localhost:4566
