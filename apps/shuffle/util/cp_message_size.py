#!/usr/bin/python3

# Copyright (c) 2020 Stanford University
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

# This cperf benchmark computes shuffle performance as a function of the
# message size (all messages are equal) and the cluster size.

from cperf import *

parser = get_parser(description='Shuffle throughput as a function of '
        'the message size and cluster size.',
        usage='%(prog)s [options]', defaults={})
parser.usage = parser.format_help()
options = parser.parse_args()

init(options)

message_sizes = list(range(1000, 10000, 1000))
message_sizes += list(range(10000, 50000, 5000))
if not options.plot_only:
    for cluster_size in range(2, options.num_nodes + 1):
        nodes = range(1, cluster_size + 1)
        command = "%s/%s %s --ifname %s --num-nodes %d --master-addr %s" \
                  " --log-file %s" % \
                  (options.shuffle_dir, options.shuffle_bin,
                   options.caladan_cfg, options.ifname, cluster_size,
                   options.master_addr, options.log_file)

        try:
            log("Starting nodes with command:\n%s" % command)
            start_nodes(nodes, command)

            run_bench_prefix = "run_bench --protocol udp --udp-port 5002 "
            for msg_size in message_sizes:
                do_cmd("gen_workload --avg-msg-size " + str(msg_size), nodes)
                do_cmd(run_bench_prefix + "--policy LRPT --times 20", nodes)

            log("Stopping nodes")
            stop_nodes()
        except Exception as e:
            log("Caught exception:\n\t" + str(e))
            log("Cleaning up orphaned '%s' processes" % options.shuffle_bin)
            do_ssh(["pkill", options.shuffle_bin], nodes)

# Parse the log files to extract useful data
out = open(options.log_dir + "/cp_message_size.data", "w")
out.write("nodes avg_msg_bytes throughput_gbps\n")
for line in open(options.log_dir + "/rc01.log", "r"):
    match = re.match('.*collected ([0-9.]+).*'
                     'policy ([a-zA-Z]+).*'
                     'max active ([0-9]+).*'
                     'cluster size ([0-9.]+).*'
                     'avg. msg size ([0-9.]+).*'
                     'msg skewness ([0-9.]+).*'
                     'part. skewness ([0-9.]+).*'
                     'throughput ([0-9.]+)', line)
    if match:
        samples = int(match.group(1))
        policy = match.group(2)
        max_active = match.group(3)
        cluster_size = int(match.group(4))
        avg_msg_size = int(match.group(5))
        msg_skew = float(match.group(6))
        part_skew = float(match.group(7))
        throughput = float(match.group(8))
        out.write("%d %d %.1f\n" % (cluster_size, avg_msg_size, throughput))
