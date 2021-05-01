#!/bin/bash

OP_ID=$1
CLOCK_KHZ=$2
LOG_DIR=$3

for x in $LOG_DIR/rc*
do
  rc_node=$(echo $x | grep -o "rc..")
  ./gen_dashboard.sh $OP_ID $CLOCK_KHZ $rc_node > /dev/null
done
egrep -h "node-.*(sending|receiving) bytes|idle_cyc" rc*_${OP_ID}.log > tt.log