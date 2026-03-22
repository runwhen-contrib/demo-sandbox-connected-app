#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-sandbox-admins-connected-app-dev-account}"
BUCKET="sandbox-eks-data-west"
S3_KEY="lambda/processor.zip"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR=$(mktemp -d)
trap "rm -rf $WORK_DIR" EXIT

echo "Packaging Lambda handler..."
cp "$SCRIPT_DIR/index.py" "$WORK_DIR/"
cd "$WORK_DIR"
zip -j processor.zip index.py

echo "Uploading to s3://$BUCKET/$S3_KEY ..."
aws s3 cp processor.zip "s3://$BUCKET/$S3_KEY" --profile "$PROFILE"

echo "Done. Delete and recreate the Lambda Function in Crossplane to pick up the new code,"
echo "or use: aws lambda update-function-code --function-name sandbox-eks-processor --s3-bucket $BUCKET --s3-key $S3_KEY --profile $PROFILE"
