#!/usr/bin/env python3

import re
import sys
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import collections as mc
import matplotlib.font_manager as font_manager

output_pdf = 'networkUtilPlot'
if len(sys.argv) == 4:
    output_pdf = sys.argv[3]
elif len(sys.argv) != 3:
    print("Usage: networkUtilPlot.py [tt_file] [num_nodes] [output_file]")
    exit(0)
tt_file = sys.argv[1]
num_nodes = int(sys.argv[2])

node_id = None
idle_cyc = None
max_ts = 0
rx_busy_segs = []
tx_busy_segs = []
cpu_idle_segs = []
rx_colors = []
tx_colors = []
cpu_colors = []

out_pkts = {i:([],[],[]) for i in range(num_nodes)}
in_pkts = {i:([],[],[]) for i in range(num_nodes)}

bytes_per_us = 25.0 / 8 * 1e3   # 25Gbps network
color_map = plt.get_cmap('tab20b', num_nodes)(range(num_nodes))
with open(tt_file, 'r') as file:
    for line in file:
        match = re.match('[ 0-9]* \| ([ 0-9.]+) us.*node-([0-9]*): (sending|receiving) bytes ([0-9]*)-([0-9]*).*node-([0-9]*)', line)
        if match:
            time_us = float(match.group(1))
            node0 = int(match.group(2))
            node1 = int(match.group(6))
            send_op = match.group(3) == "sending"
            bytes_start = int(match.group(4))
            bytes_end = int(match.group(5))

            if node0 != node_id:
                idle_cyc = None
            node_id = node0
            if bytes_start == bytes_end:
                continue
            else:
                pkt_time_us = (bytes_end - bytes_start) / bytes_per_us

            max_ts = max(max_ts, time_us + pkt_time_us)
            if send_op:
                tx_times, lens, targets = out_pkts[node0]
                tx_times.append(time_us)
                lens.append(pkt_time_us)
                targets.append(node1)
            else:
                rx_times, lens, sources = in_pkts[node0]
                rx_times.append(time_us)
                lens.append(pkt_time_us)
                sources.append(node1)
        else:
            match = re.match('[ 0-9]* \| ([ 0-9.]+) us.*idle_cyc ([0-9]*)', line)
            if not match:
                continue
            time_us = float(match.group(1))
            prev_idle_cyc = idle_cyc
            idle_cyc = int(match.group(2))
            if prev_idle_cyc is not None and idle_cyc > prev_idle_cyc:
                idle_us = (idle_cyc - prev_idle_cyc) / 2394.0
                cpu_idle_segs.append(
                    [(time_us - idle_us, node_id), (time_us, node_id)])
                cpu_colors.append('gray')

tx_util = {i:0 for i in range(num_nodes)}
rx_util = {i:0 for i in range(num_nodes)}

# Adjust the starting times of outbound packets to avoid overlap
for node_id, (times, lens, targets) in out_pkts.items():
    for i in range(1, len(times)):
        times[i] = max(times[i - 1] + lens[i - 1], times[i])
    # Compute uplink utilization
    prev_t = 0
    for i in range(len(times)):
        tx_util[node_id] += times[i] - prev_t
        prev_t = times[i] + lens[i]
    tx_util[node_id] = 1 - tx_util[node_id] / (times[-1] + lens[-1])
    # Add an outbound packet segment with its color
    for i in range(len(times)):
        t = times[i]
        tx_busy_segs.append([(t, node_id), (t + lens[i], node_id)])
        tx_colors.append(color_map[targets[i]])
# Adjust the finishing times of inbound packets to avoid overlap
for node_id, (times, lens, sources) in in_pkts.items():
    for i in reversed(range(len(times) - 1)):
        times[i] = min(times[i + 1] - lens[i + 1], times[i])
    # Compute downlink utilization
    prev_t = 0
    for i in range(len(times)):
        rx_util[node_id] += times[i] - lens[i] - prev_t
        prev_t = times[i]
    rx_util[node_id] = 1 - rx_util[node_id] / times[-1]
    # Add an inbound packet segment with its color
    for i in range(len(times)):
        t = times[i]
        rx_busy_segs.append([(t - lens[i], node_id), (t, node_id)])
        rx_colors.append(color_map[sources[i]])

fig, (ax1, ax2, ax3) = plt.subplots(3, 1)
for ax in [ax1, ax2, ax3]:
    ax.set_xlim(0, max_ts)
    ax.set_ylim(-1, num_nodes)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(1))
    ax.xaxis.set_major_locator(ticker.MultipleLocator(100))
    ax.xaxis.set_minor_locator(ticker.MultipleLocator(10))
    ax.tick_params(axis='both', which='major', labelsize='x-small')
fig.tight_layout(pad=2, h_pad=0.05)

# Create a legend to indicate which color represents which node
for node_id in range(num_nodes):
    ax1.plot([-1], [node_id], c=color_map[node_id], label=str(node_id),
             linewidth=5)
# legend_font = font_manager.FontProperties(weight='bold', style='normal',
#                                    size='xx-small')
ax1.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=10,
           fontsize='xx-small')

ax1.set_ylabel("Receiver ID", fontsize='x-small')
ax2.set_ylabel("Sender ID", fontsize='x-small')
ax3.set_ylabel("Node ID", fontsize='x-small')
ax3.set_xlabel("Time (us)", fontsize='x-small')
ax1.add_collection(
    mc.LineCollection(rx_busy_segs, colors=rx_colors, linewidths=5))
ax2.add_collection(
    mc.LineCollection(tx_busy_segs, colors=tx_colors, linewidths=5))
ax3.add_collection(
    mc.LineCollection(cpu_idle_segs, colors=cpu_colors, linewidths=5))

for node_id in range(num_nodes):
    ax1.text(max_ts, node_id - .25, '%.1f%%' % (rx_util[node_id] * 100),
             color='red', fontsize='xx-small')
    ax2.text(max_ts, node_id - .25, '%.1f%%' % (tx_util[node_id] * 100),
             color='red', fontsize='xx-small')

# print(plt.get_backend())
# plt.show()
plt.savefig("%s.pdf" % output_pdf)
