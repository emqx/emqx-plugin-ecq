#!/bin/bash

# Run command emqx ctl ecq ...
# in the given node.
# 1 for node1.emqx.io, 2 for node2.emqx.io, 3 for node3.emqx.io

set -euo pipefail

cd -P -- "$(dirname -- "$0")/../"

case "$1" in
    "1")
        NODE="node1.emqx.io"
        ;;
    "2")
        NODE="node2.emqx.io"
        ;;
    "3")
        NODE="node3.emqx.io"
        ;;
    *)
        echo "Invalid node number"
        exit 1
        ;;
esac
shift

docker exec $NODE emqx ctl ecq $@
