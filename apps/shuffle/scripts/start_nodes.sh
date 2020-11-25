#!/bin/bash

# Usage: start_nodes.sh [SHUFFLE_NODE_ARGS...] [SHUFFLE_BIN] [LOG_DIR]
PORT_START=$3
NUM_NODES=$4
SHUFFLE_BIN=$6
LOG_DIR=$7

LOG_PREFIX=$LOG_DIR/$(hostname)
((NUM_NODES = NUM_NODES - 1))
for i in $(eval echo "{0..$NUM_NODES}"); do
    ((PORT = PORT_START + i))
    COMMAND="nohup $SHUFFLE_BIN --caladan-config $1 --ifname $2 --port $PORT
        --num-nodes $4 --master-addr $5
        1>$LOG_PREFIX-$i.log 2>$LOG_PREFIX-$i.err &"
    eval $COMMAND
done
