#!/bin/bash

OP_ID=$1
CLOCK_KHZ=$2

shuffle_log="shuffle_op$OP_ID.log"
egrep "op $((OP_ID-1)) completed in" logs/latest/rc01.log -A 1000 | \
    grep "udp_shuffle: invoked" -A 1000 | \
    egrep "op $OP_ID completed in" -B 1000 | \
    ./ttmerge.py $CLOCK_KHZ > shuffle_log

cpu_ids=$(grep -o "\[CPU.*\]" shuffle_log | sort | uniq | grep -o "[0-9]*")
IFS=$'\n'

for cpu_id in $cpu_ids; do
  cpu_log="cpu$cpu_id.log"
  grep "CPU $cpu_id" shuffle_log | ./ttmerge.py $CLOCK_KHZ > $cpu_log

  echo "=== cpu $cpu_id ==="
  tail -n 1 $cpu_log | awk '{print "total:", $3}'
  # compute sched cycles: cycles spent in scheduler context?
  grep -o "sched_cyc [0-9]*" $cpu_log | awk -v khz=$CLOCK_KHZ '{if (x == 0) x = $2; y = $2} END {print "sched:", (y-x)/khz}'
  # TODO: wtf is prog_cyc? sometimes it's equal to the total time?! how is it possible? I thought prog_cyc + sched_cyc = total_cyc
  grep -o "prog_cyc [0-9]*" $cpu_log | awk -v khz=$CLOCK_KHZ '{if (x == 0) x = $2; y = $2} END {print "prog:", (y-x)/khz}'
  # compuate idle cycles: cycles spent in sched looking for jobs? (part of sched_cyc or not?)
  grep "idle_cyc" $cpu_log | awk '{if ($3 > 1.0) print $0}' | \
      grep -o "idle_cyc [0-9]*" | awk -v khz=$CLOCK_KHZ '{x += $2} END {print "  idle:", x/khz}'
  # compute softirq cycles: cycles spent int softirq_fn (so, part of prog_cyc?)
  grep -o "softirq_cyc [0-9]*" $cpu_log | awk -v khz=$CLOCK_KHZ '{if (x == 0) x = $2; y = $2} END {print "  softirq:", (y-x)/khz}'
  # compute app cycles??? = prog_cycles - softirq cycles??? no; this must be extracted from the log
  # FIXME: how to compute app cycles???
  grep "sched:" -B 1 $cpu_log | grep "sched:\|node" | ./ttmerge.py $CLOCK_KHZ | \
      grep "node" | grep -o "+ [0-9 .]* us" | awk '{sum += $2} END {print "  app:", sum}'
done