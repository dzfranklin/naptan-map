#!/usr/bin/env bash
set -euox pipefail

IMAGE_TAG=544470575466.dkr.ecr.eu-central-1.amazonaws.com/naptan-map:latest
REGION=eu-central-1
FUNCTION=arn:aws:lambda:eu-central-1:544470575466:function:generate-naptan-map

aws ecr get-login-password --region eu-central-1 | \
  docker login --username AWS --password-stdin 544470575466.dkr.ecr.eu-central-1.amazonaws.com

docker buildx build --platform linux/arm64 --provenance=false --push -t "$IMAGE_TAG" .
aws lambda update-function-code --region "$REGION" --function-name "$FUNCTION" --image-uri "$IMAGE_TAG" --no-cli-pager

aws lambda invoke  --region "$REGION" --function-name "$FUNCTION" \
  --invocation-type Event \
  --no-cli-pager \
  --payload '{}' /dev/null
