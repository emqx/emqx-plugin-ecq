#!/bin/bash

set -euo pipefail

# Smoke test: send a batch, mocking SDV platform

DATA="Testdata $(date)"

mqttx pub -h 127.0.0.1 -p 1883 -t "\$ECQ/w/client1/key1" -m "$DATA"
mqttx pub -h 127.0.0.1 -p 1883 -t "\$ECQ/w/client2/key1" -m "$DATA"
mqttx pub -h 127.0.0.1 -p 1883 -t "\$ECQ/w/client2/key2" -m "$DATA"

echo "Sent $DATA to "
echo "  client1: key1"
echo "  client2: key1, key2"
