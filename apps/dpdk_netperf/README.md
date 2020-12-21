# Latency Benchmarks

1) Build DPDK (without driver modifications)
2) Modify global variable `dpdk_port` in `dpdk_netperf.c` to select
the NIC port to be assigned to DPDK
3) Build dpdk_netperf in this directory with `make clean && make`
(`RTE_TARGET` can be overriden to select which DPDK build to link against).

## DPDK only
To run the benchmark with pure DPDK on both machines:

On the server (IP 192.168.1.2):
```
sudo ./build/dpdk_netperf -l2 --socket-mem=128 -- UDP_SERVER 192.168.1.2
```

On the client (IP 192.168.1.3):
```
sudo ./build/dpdk_netperf -l2 --socket-mem=128 -- UDP_CLIENT 192.168.1.3 192.168.1.2 50000 8001 10 8 5
```

## Shenango spinning (IOKernel + runtime)

To run Shenango with the server runtime thread spinning, start the
IOKernel and then in `shenango/apps/bench`:

```
./netbench_udp tbench.config server
```
Then run the client as above.

## Shenango waking (IOKernel + runtime + wakeup)

To run with Shenango in its default mode but no batch work, start the
IOKernel and then in `shenango/apps/bench`:
```
./netbench_udp waking.config server
```
Then run the client as above.

## Shenango preempting (IOKernel + runtime + wakeup + preemption)

To run Shenango with a batch application running concurrently, start
the IOKernel and then in `shenango/apps/bench`:
```
./stress stress.config 100 100 sqrt
./netbench_udp waking.config server
```

Then run the client as above. If your server does not have 24
hyperthreads, you will need to adjust `runtime_kthreads` in
stress.config to be 2 fewer than the number of hyperthreads on your
server.