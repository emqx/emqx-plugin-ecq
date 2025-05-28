#!/bin/bash

# Smoke test: subscribe to fanout topic, mock a vehicle.

set -euo pipefail
NUM="${1:-}"
if [ -z "$NUM" ]; then
    echo "Usage: $0 NUM"
    echo "  NUM can be 1, 2, or 3, see the client IDs in send.sh"
    exit 1
fi
CLIENTID="client${NUM}"

mqttx sub -h 127.0.0.1 -p 1883 -i "$CLIENTID" -t "\$ECQ/$CLIENTID/#" -q 1
