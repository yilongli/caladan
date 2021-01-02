#!/bin/bash

OP_ID=$1
CLOCK_KHZ=$2

shuffle_log="shuffle_op$OP_ID.log"
egrep "op $((OP_ID-1)) completed in" logs/latest/rc01.log -A 1000 | \
    grep "udp_shuffle: invoked" -A 1000 | \
    egrep "op $OP_ID completed in" -B 1000 | \
    ./ttmerge.py $CLOCK_KHZ > $shuffle_log

cpu_ids=$(grep -o "\[CPU.*\]" $shuffle_log | sort | uniq | grep -o "[0-9]*")
IFS=$'\n'

total_cyc=()
idle_cyc=()
sched_cyc=()
prog_cyc=()
app_cyc=()
softirq_cyc=()
other_cyc=()

grpt_msg_cyc=()
tx_data_cyc=()
tx_ack_cyc=()
handle_data_cyc=()
handle_ack_cyc=()
rx_pkt_cyc=()

for cpu_id in $cpu_ids; do
  cpu_log="cpu$cpu_id.log"
  grep "CPU $cpu_id" $shuffle_log | ./ttmerge.py $CLOCK_KHZ > $cpu_log

  # Extract walltime in cycles
  total_cyc+=( "$(tail -n 1 $cpu_log | awk '{printf $3}')" )

  # Compute idle cycles: cycles spent in sched.c:schedule() looking for work
  # (but couldn't find any) some cycles are also counted towards sched_cyc
  idle_cyc+=( "$(grep -o "idle_cyc [0-9]*" $cpu_log |
      awk -v khz=$CLOCK_KHZ '{if (x == 0) x = $2; y = $2} END {print (y-x)/khz}')" )

  # Compute sched cycles: cycles spent in the scheduler context
  sched_cyc+=( "$(grep -o "sched_cyc [0-9]*" $cpu_log |
      awk -v khz=$CLOCK_KHZ '{if (x == 0) x = $2; y = $2} END {print (y-x)/khz}' )" )

  # Compute prog cycles: cycles spent in the uthread context (including app
  # code and softirqs); for spinning kthreads, prog_cycles + sched_cycles is
  # roughly equal to total cycles
  x0=$(grep "prog_cyc [0-9]*" $cpu_log | head -n 1 | grep -o "| [0-9 .]* us" | awk '{print $2}')
  prog_cyc+=( "$(grep -o "prog_cyc [0-9]*" $cpu_log | \
      awk -v khz="$CLOCK_KHZ" -v x0="$x0" '{if (x == 0) x = $2; y = $2} END {print x0+(y-x)/khz}')" )

  # Compute app cycles: cycles spent in app code and runtime functions that run
  # within the uthread context (e.g., sender-side udp/tcp protocol processing).
  # app_cyc is currently computed by analyzing the time trace events: extract
  # all intervals that begin with "node-n:" and end with "sched: " and add up
  # the interval cycles.
  app_cyc+=( "$(egrep "sched: (enter|finally)" -A 1 $cpu_log | grep "sched:\|node" | \
      ./ttmerge.py $CLOCK_KHZ | grep "node" -A 1 | grep "sched:" | \
      grep -o "+ [0-9 .]* us" | awk -v x0=$x0 '{sum += $2} END {print sum+x0}')" )

  # Compute softirq cycles: cycles spent in softirq_fn (i.e., part of prog_cyc)
  # There are two ways to compute softirq_cyc: 1) use the stat counter directly
  # maintained by caladan and 2) analyze time trace events like app_cyc.
  # The second approach is preferred here because rdtsc() is used to track
  # cycles spent in softirq_fn() but it tends to not cover the last cache miss
  # (so the cache miss time actually showed up between "softirq_fn: finished"
  # and "sched: enter").
#  softirq_cyc+=( "$(grep -o "softirq_cyc [0-9]*" $cpu_log | \
#      awk -v khz=$CLOCK_KHZ '{if (x == 0) x = $2; y = $2} END {print (y-x)/khz}')" )
  softirq_cyc+=( "$(egrep "sched: (enter|finally)" -A 1 $cpu_log | grep "sched:\|softirq_fn:" | \
      ./ttmerge.py $CLOCK_KHZ | grep "softirq_fn:" -A 1 | grep "sched:" | \
      grep -o "+ [0-9 .]* us" | awk '{sum += $2} END {print sum}')" )

  other_cyc+=( "$(echo "${prog_cyc[-1]}-${app_cyc[-1]}-${softirq_cyc[-1]}" | bc -l)" )

  line="$(grep "cpu $cpu_id" $shuffle_log)"
  grpt_msg_cyc+=( "$(echo "$line" | grep -o "grpt_msg_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{print $2/khz}' )" )
  tx_data_cyc+=( "$(echo "$line" | grep -o "tx_data_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{print $2/khz}' )" )
  tx_ack_cyc+=( "$(echo "$line" | grep -o "tx_ack_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{print $2/khz}' )" )
  handle_data_cyc+=( "$(echo "$line" | grep -o "handle_data_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{print $2/khz}' )" )
  handle_ack_cyc+=( "$(echo "$line" | grep -o "handle_ack_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{print $2/khz}' )" )
  rx_pkt_cyc+=( "$(echo "${app_cyc[-1]} - ${grpt_msg_cyc[-1]} - \
      ${tx_data_cyc[-1]} - ${tx_ack_cyc[-1]} - ${handle_data_cyc[-1]} - \
      ${handle_ack_cyc[-1]}" | bc -l)" )
done

# Print the time breakdown table
cpus=( $cpu_ids )
printf "%s" '----------'; for x in "${cpus[@]}"; do printf "%s" '--------'; done; echo
printf "             "; for x in "${cpus[@]}"; do printf "%8s" "CPU $x"; done; echo
printf "total:       "; for x in "${total_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "idle:        "; for x in "${idle_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "sched:       "; for x in "${sched_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "prog:        "; for x in "${prog_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "  app:       "; for x in "${app_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "    grpt:    "; for x in "${grpt_msg_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "    tx_data: "; for x in "${tx_data_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "    tx_ack:  "; for x in "${tx_ack_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "    hdl_data:"; for x in "${handle_data_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "    hdl_ack: "; for x in "${handle_ack_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "    rx_pkt?  "; for x in "${rx_pkt_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "  softirq:   "; for x in "${softirq_cyc[@]}"; do printf "%8.2f" "$x"; done; echo
printf "  other:     "; for x in "${other_cyc[@]}"; do printf "%8.2f" $x; done; echo
