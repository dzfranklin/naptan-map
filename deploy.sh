#!/usr/bin/env bash
set -euox pipefail

aws ecr get-login-password --region eu-central-1 | \
  docker login --username AWS --password-stdin 544470575466.dkr.ecr.eu-central-1.amazonaws.com

IMAGE_TAG=544470575466.dkr.ecr.eu-central-1.amazonaws.com/naptan-map:latest
docker buildx build --platform linux/arm64 --provenance=false --push -t "$IMAGE_TAG" .
aws lambda update-function-code \
  --region eu-central-1 --function-name generate-naptan-map \
  --image-uri "$IMAGE_TAG" \
  --no-cli-pager
