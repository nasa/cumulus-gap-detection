#!/bin/bash

cd "$(dirname "$0")/.."  # Go to project root

# Setup symlink for utils module
mkdir -p utils
ln -sf $(pwd)/src/shared/utils.py $(pwd)/utils/__init__.py

if [ "$1" = "-d" ]; then
    #docker compose -f test/docker-compose.yaml up 
    docker compose -f test/docker-compose.yaml up --build
else
    docker compose -f test/docker-compose.yaml down -v
    docker compose -f test/docker-compose.yaml up --build --abort-on-container-exit
fi
