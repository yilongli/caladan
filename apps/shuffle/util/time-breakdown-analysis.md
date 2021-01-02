
For each stage, sum up the cycles spent on each cpu:
```
awk '{if ($2 > 1.0) { printf "%s%8.2f\n", $0, $2 + $3 } else { printf "%s\n", $0 } }' dashboard.txt
```

Check the variance of the sums across runs:
```

```
While the work split on different cpus varied a lot due to dynamic load 
balancing, the sums seems quite stable.


TODO: what about the time breakdown within RX and TX thread?
TODO: which one needs to do more work? TX or RX? what does it imply? 

Get a timeline which includes when each outbound data packets are sent (used to
find bubbles in the uplink):
```
grep "sending bytes" rc01_op9.log | ./ttmerge 2394
```

Get a timeline which includes when each inbound data packets are received (used
to find bubbles in the downlink):
```
grep "receiving bytes" rc02_op9.log | ./ttmerge 2394
```

TODO: what can we find by comparing the two timelines???