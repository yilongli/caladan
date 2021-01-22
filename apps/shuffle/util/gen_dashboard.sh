#!/bin/bash

OP_ID=$1
CLOCK_KHZ=$2

shuffle_log="shuffle_op$OP_ID.log"
egrep "op $((OP_ID-1)) completed in" logs/latest/rc01.log -A 2000 | \
    grep "udp_shuffle: invoked" -A 2000 | \
    egrep "op $OP_ID completed in" -B 2000 | \
    ./ttmerge.py $CLOCK_KHZ > $shuffle_log

cpu_ids=$(grep -o "\[CPU.*\]" $shuffle_log | sort | uniq | grep -o "[0-9]*")
IFS=$'\n'

total_cyc=()
idle_cyc=()
sched_cyc=()
prog_cyc=()
app_cyc=()
softirq_cyc=()
softirq_net_rx_cyc=()
prog_unknown_cyc=()

tx_thread_cyc=()
grpt_msg_cyc=()
tx_data_cyc=()
rx_thread_data_cyc=()
handle_data_cyc=()
tx_ack_cyc=()
rx_thread_ack_cyc=()
handle_ack_cyc=()
local_copy_cyc=()
app_unknown_cyc=()

for cpu_id in $cpu_ids; do
  cpu_log="cpu$cpu_id.log"
  grep "CPU $cpu_id" $shuffle_log | ./ttmerge.py $CLOCK_KHZ > $cpu_log

  # Filter out irrelevant events so that it will be less likely to break
  # the analysis below which relies on commands like "grep -A ? -B ?" when
  # adding new tt_records()'s in the program.
  tmp_log="/tmp/$cpu_log"
  egrep "sched: (enter|finally|switch)|softirq_fn:|node-[0-9]*:" $cpu_log | \
      ./ttmerge.py $CLOCK_KHZ > $tmp_log

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
  # TODO: how to double check if app_cyc is computed correctly? what if the timetrace has a different format? seems a bit fragile
  app_cyc+=( "$(egrep "sched: (enter|finally|switch)" -A 1 $tmp_log | grep "sched:\|node" | \
      ./ttmerge.py $CLOCK_KHZ | grep "node" -A 1 | grep "sched:" | \
      grep -o "+ [0-9 .]* us" | awk -v x0=$x0 '{sum += $2} END {print sum+x0}')" )

  # Compute softirq cycles: cycles spent in softirq_fn (i.e., part of prog_cyc)
  # There are two ways to compute softirq_cyc: 1) use the stat counter directly
  # maintained by caladan and 2) analyze time trace events like app_cyc.
  # The second approach is preferred here because rdtsc() is used to track
  # cycles spent in softirq_fn() but it tends to not cover the last cache miss
  # (so the cache miss time actually showed up between "softirq_fn: finished"
  # and "sched: enter").
  # TODO: now I suspect that the divergence is in fact due to the recycle
  # overhead of uthread, as opposed to cache misses.
#  softirq_cyc+=( "$(grep -o "softirq_cyc [0-9]*" $tmp_log | \
#      awk -v khz=$CLOCK_KHZ '{if (x == 0) x = $2; y = $2} END {print (y-x)/khz}')" )
  softirq_cyc+=( "$(egrep "sched: (enter|finally|switch)" -A 1 $tmp_log | grep "sched:\|softirq_fn: invoked" | \
      ./ttmerge.py $CLOCK_KHZ | grep "softirq_fn:" -A 1 | grep "sched:" | \
      grep -o "+ [0-9 .]* us" | awk -v sum=0 '{sum += $2} END {print sum}')" )
  softirq_net_rx_cyc+=( "$(egrep "softirq_fn: (invoked|net_rx done)" $tmp_log | \
      ./ttmerge.py $CLOCK_KHZ | grep "softirq_fn: net_rx done" | \
      grep -o "+ [0-9 .]* us" | awk -v sum=0 '{sum += $2} END {print sum}')" )

  # Similar to computing softirq_cyc directly from time trace, we can also do
  # the same for cycles spent on receiving DATA/ACK packets; this is useful to
  # verify the numbers gathered using in-program perf. counters and get a sense
  # on the overhead of uthread cleanup.
  rx_thread_data_cyc+=( "$(egrep "sched: (enter|finally|switch)" -A 1 $tmp_log | grep "sched:\|receiving bytes" | \
      ./ttmerge.py $CLOCK_KHZ | grep "node" -A 1 | grep "sched:" | \
      grep -o "+ [0-9 .]* us" | awk -v sum=0 '{sum += $2} END {print sum}')" )
  rx_thread_ack_cyc+=( "$(egrep "sched: (enter|finally|switch)" -A 1 $tmp_log | grep "sched:\|received ACK" | \
      ./ttmerge.py $CLOCK_KHZ | grep "node" -A 1 | grep "sched:" | \
      grep -o "+ [0-9 .]* us" | awk -v sum=0 '{sum += $2} END {print sum}')" )

  # TODO: what other overhead? (softirq_gather is already counted in sched_cyc?)
  prog_unknown_cyc+=( "$(echo "${prog_cyc[-1]} ${app_cyc[-1]} ${softirq_cyc[-1]}" | \
      awk '{print $1-$2-$3}' )" )

  line="$(grep "cpu $cpu_id" $shuffle_log)"
  grpt_msg_cyc+=( "$(echo "$line" | grep -o "grpt_msg_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{v=1; print $2/khz} END {if (!(v)) print 0}' )" )
  tx_data_cyc+=( "$(echo "$line" | grep -o "tx_data_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{v=1; print $2/khz} END {if (!(v)) print 0}' )" )
  tx_thread_cyc+=( "$(echo "${grpt_msg_cyc[-1]} ${tx_data_cyc[-1]}" | awk '{print $1+$2}')" )
  handle_data_cyc+=( "$(echo "$line" | grep -o "handle_data_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{v=1; print $2/khz} END {if (!(v)) print 0}' )" )
  tx_ack_cyc+=( "$(echo "$line" | grep -o "tx_ack_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{v=1; print $2/khz} END {if (!(v)) print 0}' )" )
  handle_ack_cyc+=( "$(echo "$line" | grep -o "handle_ack_cycles [0-9]*" | \
      awk -v khz="$CLOCK_KHZ" '{v=1; print $2/khz} END {if (!(v)) print 0}' )" )
  local_copy_cyc+=( "$(grep -o "local_copy_cycles [0-9]*" $cpu_log | \
      awk -v khz="$CLOCK_KHZ" '{v=1; print $2/khz} END {if (!(v)) print 0}' )" )
  app_unknown_cyc+=( "$(echo "${app_cyc[-1]} ${grpt_msg_cyc[-1]} ${tx_data_cyc[-1]} \
      ${rx_thread_data_cyc[-1]} ${rx_thread_ack_cyc[-1]} ${local_copy_cyc[-1]}" | \
      awk '{printf "%.2f", $1-$2-$3-$4-$5-$6}' )" )

  tx_data_pkt+=( "$(echo "$line" | grep -o "tx_data_pkts [0-9]*" | awk '{v=1; print $2z} END {if (!(v)) print 0}' )" )
  tx_ack_pkt+=( "$(echo "$line" | grep -o "tx_ack_pkts [0-9]*" | awk '{v=1; print $2z} END {if (!(v)) print 0}' )" )
  rx_data_pkt+=( "$(echo "$line" | grep -o "rx_data_pkts [0-9]*" | awk '{v=1; print $2z} END {if (!(v)) print 0}' )" )
  rx_ack_pkt+=( "$(echo "$line" | grep -o "rx_ack_pkts [0-9]*" | awk '{v=1; print $2z} END {if (!(v)) print 0}' )" )
done

function array_sum() {
  arr=("$@")
  IFS='+' result=$(echo "scale=1; ${arr[*]}" | bc)
  echo "$result"
}

# TODO: how to implement the following as a loop over array names?
total_cyc+=( "$(array_sum "${total_cyc[@]}")" )
idle_cyc+=( "$(array_sum "${idle_cyc[@]}")" )
sched_cyc+=( "$(array_sum "${sched_cyc[@]}")" )
prog_cyc+=( "$(array_sum "${prog_cyc[@]}")" )
app_cyc+=( "$(array_sum "${app_cyc[@]}")" )
tx_thread_cyc+=( "$(array_sum "${tx_thread_cyc[@]}")" )
grpt_msg_cyc+=( "$(array_sum "${grpt_msg_cyc[@]}")" )
tx_data_cyc+=( "$(array_sum "${tx_data_cyc[@]}")" )
tx_data_pkt+=( "$(array_sum "${tx_data_pkt[@]}")" )
tx_ack_cyc+=( "$(array_sum "${tx_ack_cyc[@]}")" )
tx_ack_pkt+=( "$(array_sum "${tx_ack_pkt[@]}")" )
rx_data_cyc+=( "$(array_sum "${rx_data_cyc[@]}")" )
rx_data_pkt+=( "$(array_sum "${rx_data_pkt[@]}")" )
rx_thread_data_cyc+=( "$(array_sum "${rx_thread_data_cyc[@]}")" )
rx_thread_ack_cyc+=( "$(array_sum "${rx_thread_ack_cyc[@]}")" )
rx_ack_pkt+=( "$(array_sum "${rx_ack_pkt[@]}")" )
handle_ack_cyc+=( "$(array_sum "${handle_ack_cyc[@]}")" )
handle_data_cyc+=( "$(array_sum "${handle_data_cyc[@]}")" )
softirq_cyc+=( "$(array_sum "${softirq_cyc[@]}")" )
softirq_net_rx_cyc+=( "$(array_sum "${softirq_net_rx_cyc[@]}")" )
app_unknown_cyc+=( "$(array_sum "${app_unknown_cyc[@]}")" )
prog_unknown_cyc+=( "$(array_sum "${prog_unknown_cyc[@]}")" )


# Another way to compute the total time spent in the TX thread;
# this can be used to double-check the sum of tx_thread_cyc on each cpu.
tx_thread_cyc+=( "$(grep -o "TX thread done, busy_cyc [0-9]*" $shuffle_log | \
    awk -v khz="$CLOCK_KHZ" '{print $5/khz}' )" )

# Print the time breakdown table
cpus=( $cpu_ids "**" )
printf "%s" '---------------'; for x in "${cpus[@]}"; do printf "%s" '--------'; done; echo
echo "op_id = $OP_ID"
printf "               "; for x in "${cpus[@]}"; do printf "%8s" "CPU $x"; done; echo
printf "total:         "; for x in "${total_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "idle:          "; for x in "${idle_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "sched:         "; for x in "${sched_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "prog:          "; for x in "${prog_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "  app:         "; for x in "${app_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "    tx_thread: "; for x in "${tx_thread_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "      grpt:    "; for x in "${grpt_msg_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "      tx_data: "; for x in "${tx_data_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "    rx_ack:    "; for x in "${rx_thread_ack_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "      hdl_ack: "; for x in "${handle_ack_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "      thr_fin: "; for i in "${!rx_thread_ack_cyc[@]}"; do \
    printf "%8.2f" "$(echo "${rx_thread_ack_cyc[$i]} ${handle_ack_cyc[$i]}" |  awk '{print $1-$2}')"; done; echo
printf "    rx_data:   "; for x in "${rx_thread_data_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "      hdl_data:"; for x in "${handle_data_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "      tx_ack:  "; for x in "${tx_ack_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "      thr_fin: "; for i in "${!rx_thread_data_cyc[@]}"; do \
    printf "%8.2f" "$(echo "${rx_thread_data_cyc[$i]} ${handle_data_cyc[$i]} ${tx_ack_cyc[$i]}" |  awk '{print $1-$2-$3}')"; done; echo
#printf "    local_copy:"; for x in "${local_copy_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "    unknown:   "; for x in "${app_unknown_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "  softirq:     "; for x in "${softirq_cyc[@]}"; do printf "%8.2f" "$x"; done; echo
printf "    net_rx:    "; for x in "${softirq_net_rx_cyc[@]}"; do printf "%8.2f" "$x"; done; echo
printf "  other:       "; for x in "${prog_unknown_cyc[@]}"; do printf "%8.2f" $x; done; echo
echo

function print_numbers() {
  fmt="$1"
  shift
  ns=("$@")
  for x in "${ns[@]}"; do printf "$fmt" $x; done
  echo
#  printf "$fmt\n" "$(array_sum "${ns[@]}")"
}

printf "            "; for x in "${cpus[@]}"; do printf "%8s" "CPU $x"; done; echo
printf "tx_data:    "; print_numbers "%8.2f" "${tx_data_cyc[@]}"
printf "  pkts:     "; print_numbers "%8.0f" "${tx_data_pkt[@]}"
#for x in "${tx_data_pkt[@]}"; do printf "%8.0f" $x; done; echo
printf "  cost_ns:  "; for i in "${!tx_data_pkt[@]}"; do \
    printf "%8.0f" "$(echo "${tx_data_cyc[$i]} ${tx_data_pkt[$i]}" |  awk '{print $1/$2*1000}')"; done; echo
#    printf "%8.0f"
printf "tx_ack:     "; for x in "${tx_ack_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "  pkts:     "; for x in "${tx_ack_pkt[@]}"; do printf "%8.0f" $x; done; echo
printf "  cost_ns:  "; for i in "${!tx_ack_pkt[@]}"; do \
    printf "%8.0f" "$(echo "${tx_ack_cyc[$i]} ${tx_ack_pkt[$i]}" |  awk '{print $1/$2*1000}')"; done; echo
printf "rx_data:    "; for i in "${!rx_data_pkt[@]}"; do \
    printf "%8.2f" "$(echo "${rx_thread_data_cyc[$i]} ${tx_ack_cyc[$i]}" |  awk '{print $1-$2}')"; done; echo
printf "  pkts:     "; for x in "${rx_data_pkt[@]}"; do printf "%8.f" $x; done; echo
printf "  cost_ns:  "; for i in "${!rx_data_pkt[@]}"; do \
    printf "%8.0f" "$(echo "${rx_thread_data_cyc[$i]} ${tx_ack_cyc[$i]} ${rx_data_pkt[$i]}" |  awk '{print ($1-$2)/$3*1000}')"; done; echo
printf "rx_ack:     "; for x in "${rx_thread_ack_cyc[@]}"; do printf "%8.2f" $x; done; echo
printf "  pkts:     "; for x in "${rx_ack_pkt[@]}"; do printf "%8.0f" $x; done; echo
printf "  cost_ns:  "; for i in "${!rx_ack_pkt[@]}"; do \
    printf "%8.0f" "$(echo "${rx_thread_ack_cyc[$i]} ${rx_ack_pkt[$i]}" |  awk '{print $1/$2*1000}')"; done; echo


