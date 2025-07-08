#!/bin/bash
set -euo pipefail
lambda_name=$1
lambda_path=$2

# Shared dependency packaging
if [ "$lambda_name" == "shared" ]; then
  rm -rf python/
  mkdir -p python/utils
  touch python/utils/__init__.py
  if [ -d "$lambda_path" ]; then
    cp "$lambda_path"/*.py python/utils/
  fi
  if [ -f "$lambda_path/requirements.txt" ]; then
    pip install \
      -r "$lambda_path/requirements.txt" \
      --target=python/ \
      --platform manylinux2014_x86_64 \
      --only-binary=:all: \
      --no-cache-dir \
      --upgrade
  fi
  find python/ -name "*.pyc" -delete
  deterministic-zip -r /artifacts/layers/utils-deps.zip python/
  echo "Shared utilities layer packaging completed"

# Regular lambda packaging
else
  rm -rf python/
  mkdir -p python/
  if [ -f "$lambda_path/requirements.txt" ]; then
    pip install \
      -r "$lambda_path/requirements.txt" \
      --target=python/ \
      --platform manylinux2014_x86_64 \
      --only-binary=:all: \
      --no-cache-dir \
      --upgrade
    find python/ -name "*.pyc" -delete
    deterministic-zip -r /artifacts/layers/$lambda_name-deps.zip python/
    echo "Dependencies packaging completed for $lambda_name"
  else
    echo "No requirements.txt found, skipping dependencies packaging"
  fi
  echo "Packaging source code for $lambda_name"
  cd "$lambda_path"
  deterministic-zip -r /artifacts/functions/$lambda_name.zip $(find . -type f \( -name "*.py" -o -name "*.sql" -o -name "*.json" \))
  echo "Source packaging completed for $lambda_name"
fi