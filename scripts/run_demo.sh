#!/usr/bin/env bash
# Starts the consumer, then the producer, and reports both sides.
set -euo pipefail
cd "$(dirname "$0")/.."

COUNT="${1:-8000000}"
make -s all

./build/demo_consumer &
CONSUMER=$!
trap 'kill -0 $CONSUMER 2>/dev/null && kill $CONSUMER' EXIT

sleep 1
./build/demo_producer "$COUNT"
wait $CONSUMER
